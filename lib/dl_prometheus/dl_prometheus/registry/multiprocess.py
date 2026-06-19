from collections.abc import (
    Callable,
    Iterator,
    Sequence,
)
import logging
import os
import pathlib
import threading
from typing import ClassVar

import attrs
import prometheus_client
import prometheus_client.exposition
import prometheus_client.metrics_core
import prometheus_client.mmap_dict
import prometheus_client.multiprocess
import prometheus_client.values

import dl_prometheus.registry.base as base

LOGGER = logging.getLogger(__name__)


# Overwrite of prometheus_client.values.MultiProcessValue (v0.25.0).
#
# Changes from upstream:
# 1. Added `metrics_dir` parameter; the file directory is closure-captured
#    here instead of read from `os.environ["PROMETHEUS_MULTIPROC_DIR"]` in
#    `__reset`. This is what makes the value class env-var-free.
# 2. Dropped the `prometheus_multiproc_dir` → `PROMETHEUS_MULTIPROC_DIR`
#    deprecation shim in `__init__`; this class doesn't touch env vars.
#
# Why: prometheus_client's multiproc layer is coupled to a process-global
# env var. We want full dependency injection — the directory is supplied at
# value-class construction time and travels through closure scope into the
# per-instance `__reset` method. Everything else (file format, mmap layout,
# label encoding, PID-change handling, lock discipline) is preserved verbatim
# so that `prometheus_client.multiprocess.MultiProcessCollector` can read our
# files unchanged, and so upstream bug-fixes remain trivial to backport.
def PatchedMultiProcessValue(  # noqa: N802
    metrics_dir: pathlib.Path,
    process_identifier: Callable[[], int] = os.getpid,
) -> type:
    """Returns a MmapedValue class based on a process_identifier function.

    The 'process_identifier' function MUST comply with this simple rule:
    when called in simultaneously running processes it MUST return distinct values.

    Using a different function than the default 'os.getpid' is at your own risk.
    """
    files = {}
    values = []
    pid = {"value": process_identifier()}
    # Use a single global lock when in multi-processing mode
    # as we presume this means there is no threading going on.
    # This avoids the need to also have mutexes in __MmapDict.
    lock = threading.Lock()

    class MmapedValue:
        """A float protected by a mutex backed by a per-process mmaped file."""

        _multiprocess = True

        def __init__(
            self,
            typ: str,
            metric_name: str,
            name: str,
            labelnames: Sequence[str],
            labelvalues: Sequence[str],
            help_text: str,
            multiprocess_mode: str = "",
            **kwargs: object,
        ) -> None:
            self._params = typ, metric_name, name, labelnames, labelvalues, help_text, multiprocess_mode
            with lock:
                self.__check_for_pid_change()
                self.__reset()
                values.append(self)

        def __reset(self) -> None:
            typ, metric_name, name, labelnames, labelvalues, help_text, multiprocess_mode = self._params
            if typ == "gauge":
                file_prefix = typ + "_" + multiprocess_mode
            else:
                file_prefix = typ
            if file_prefix not in files:
                filename = os.path.join(
                    str(metrics_dir),
                    "{}_{}.db".format(file_prefix, pid["value"]),
                )

                files[file_prefix] = prometheus_client.mmap_dict.MmapedDict(filename)
            self._file = files[file_prefix]
            self._key = prometheus_client.mmap_dict.mmap_key(
                metric_name, name, list(labelnames), list(labelvalues), help_text
            )
            self._value, self._timestamp = self._file.read_value(self._key)

        def __check_for_pid_change(self) -> None:
            actual_pid = process_identifier()
            if pid["value"] != actual_pid:
                pid["value"] = actual_pid
                for f in files.values():
                    f.close()
                files.clear()
                for value in values:
                    value.__reset()

        def inc(self, amount: float) -> None:
            with lock:
                self.__check_for_pid_change()
                self._value += amount
                self._timestamp = 0.0
                self._file.write_value(self._key, self._value, self._timestamp)

        def set(self, value: float, timestamp: float | None = None) -> None:
            with lock:
                self.__check_for_pid_change()
                self._value = value
                self._timestamp = timestamp or 0.0
                self._file.write_value(self._key, self._value, self._timestamp)

        def set_exemplar(self, exemplar: object) -> None:
            return

        def get(self) -> float:
            with lock:
                self.__check_for_pid_change()
                return self._value

        def get_exemplar(self) -> None:
            return None

    return MmapedValue


@attrs.define(kw_only=True, eq=False, slots=False)
class MultiprocessMetricsRegistry(base.BaseMetricsRegistry):
    _metrics_dir: pathlib.Path
    _collector_registry: prometheus_client.CollectorRegistry = attrs.field(init=False)
    _closed: bool = attrs.field(init=False, default=False)

    # Class-scoped refcount: the patch to prometheus_client.values.ValueClass
    # is a process-global side effect, so we apply it on the first live
    # registry and restore the original on the last. The original is captured
    # at patch time (not import time) to play nicely with other code that may
    # itself have swapped the class. All ClassVar reads/writes go through the
    # defining class (not type(self)) so subclasses share the parent's state
    # instead of forking their own copy on first assignment.
    _instance_count: ClassVar[int] = 0
    _instance_lock: ClassVar[threading.Lock] = threading.Lock()
    _original_value_class: ClassVar[type | None] = None
    _patched_metrics_dir: ClassVar[pathlib.Path | None] = None

    def __attrs_post_init__(self) -> None:
        self._metrics_dir.mkdir(parents=True, exist_ok=True)

        cls = MultiprocessMetricsRegistry
        with cls._instance_lock:
            if cls._instance_count == 0:
                cls._original_value_class = prometheus_client.values.ValueClass
                cls._patched_metrics_dir = self._metrics_dir
                prometheus_client.values.ValueClass = PatchedMultiProcessValue(self._metrics_dir)
            elif cls._patched_metrics_dir != self._metrics_dir:
                # Raising here means we never bump the refcount; mark this
                # instance "closed" so the __del__ leak-warning doesn't fire
                # on the partial instance the caller never received.
                self._closed = True
                raise ValueError(
                    f"MultiprocessMetricsRegistry already active with "
                    f"metrics_dir={cls._patched_metrics_dir!r}; cannot construct "
                    f"another with metrics_dir={self._metrics_dir!r}. "
                    f"Only one metrics_dir is supported per process."
                )
            cls._instance_count += 1

        try:
            super().__attrs_post_init__()
            self._collector_registry = prometheus_client.CollectorRegistry()
            prometheus_client.multiprocess.MultiProcessCollector(
                registry=self._collector_registry,
                path=str(self._metrics_dir),
            )
        except BaseException:
            self.close()
            raise

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True

        cls = MultiprocessMetricsRegistry
        with cls._instance_lock:
            cls._instance_count -= 1
            if cls._instance_count == 0:
                prometheus_client.values.ValueClass = cls._original_value_class
                cls._original_value_class = None
                cls._patched_metrics_dir = None

    def __del__(self) -> None:
        # Don't try to clean up here — GC ordering at interpreter shutdown
        # makes that fragile. Instead, surface the leak: callers must
        # close() explicitly or use the context manager.
        if not self._closed:
            LOGGER.warning(
                "MultiprocessMetricsRegistry(metrics_dir=%r) was garbage-collected "
                "without close(). The patched prometheus_client.values.ValueClass "
                "will remain installed for the rest of the process lifetime.",
                self._metrics_dir,
            )

    def __enter__(self) -> "MultiprocessMetricsRegistry":
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    def iter_metrics(self) -> Iterator[prometheus_client.metrics_core.Metric]:
        if self._closed:
            raise RuntimeError("Registry is closed")
        yield from self._collector_registry.collect()

    def get_latest(self) -> base.Latest:
        if self._closed:
            raise RuntimeError("Registry is closed")
        return base.Latest(
            body=prometheus_client.exposition.generate_latest(self._collector_registry),
            content_type=prometheus_client.exposition.CONTENT_TYPE_LATEST,
        )
