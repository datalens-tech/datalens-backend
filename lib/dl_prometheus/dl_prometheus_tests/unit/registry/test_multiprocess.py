from collections.abc import (
    Callable,
    Sequence,
)
import multiprocessing
import os
import pathlib
import threading

import prometheus_client.samples
import prometheus_client.values
import pytest

import dl_prometheus
import dl_prometheus.registry.multiprocess as multiprocess_module


def _increment_counter(metrics_dir: pathlib.Path, increments: list[float]) -> None:
    counter = dl_prometheus.Counter(name="events_total", documentation="events")
    with dl_prometheus.MultiprocessMetricsRegistry(metrics_dir=metrics_dir, metrics=(counter,)):
        for amount in increments:
            counter.inc(amount)


def _increment_labeled_counter(metrics_dir: pathlib.Path, label_value: str, amount: float) -> None:
    counter = dl_prometheus.Counter(
        name="events_total",
        documentation="events",
        labelnames=("kind",),
    )
    with dl_prometheus.MultiprocessMetricsRegistry(metrics_dir=metrics_dir, metrics=(counter,)):
        counter.inc(amount, labels={"kind": label_value})


def _set_gauge_sum(metrics_dir: pathlib.Path, value: float) -> None:
    gauge = dl_prometheus.Gauge(
        name="memory_bytes",
        documentation="memory",
        multiprocess_mode=dl_prometheus.MultiprocessMode.SUM,
    )
    with dl_prometheus.MultiprocessMetricsRegistry(metrics_dir=metrics_dir, metrics=(gauge,)):
        gauge.set(value)


def _observe_histogram(metrics_dir: pathlib.Path, values: list[float]) -> None:
    histogram = dl_prometheus.Histogram(
        name="latency_seconds",
        documentation="latency",
        buckets=(1.0, 5.0),
    )
    with dl_prometheus.MultiprocessMetricsRegistry(metrics_dir=metrics_dir, metrics=(histogram,)):
        for value in values:
            histogram.observe(value)


def _observe_summary(metrics_dir: pathlib.Path, values: list[float]) -> None:
    summary = dl_prometheus.Summary(
        name="response_time_seconds",
        documentation="response time",
    )
    with dl_prometheus.MultiprocessMetricsRegistry(metrics_dir=metrics_dir, metrics=(summary,)):
        for value in values:
            summary.observe(value)


def _assert_registry_patches_value_class(metrics_dir: pathlib.Path) -> None:
    original = prometheus_client.values.ValueClass
    with dl_prometheus.MultiprocessMetricsRegistry(metrics_dir=metrics_dir, metrics=()):
        assert prometheus_client.values.ValueClass is not original


def _assert_registry_close_restores_value_class(metrics_dir: pathlib.Path) -> None:
    original = prometheus_client.values.ValueClass
    with dl_prometheus.MultiprocessMetricsRegistry(metrics_dir=metrics_dir, metrics=()):
        assert prometheus_client.values.ValueClass is not original
    assert prometheus_client.values.ValueClass is original


def _run_multiprocess_workers(target: Callable[..., None], args_per_worker: Sequence[tuple]) -> None:
    context = multiprocessing.get_context("spawn")
    processes = [context.Process(target=target, args=args) for args in args_per_worker]

    for process in processes:
        process.start()

    for process in processes:
        process.join()
        assert process.exitcode == 0


def _run_multithread_workers(target: Callable[..., None], args_per_worker: Sequence[tuple]) -> None:
    # threading.Thread swallows exceptions raised in the target — they get
    # logged to stderr but don't propagate to the calling thread, so a bare
    # `assert` inside a worker would not fail the test. Wrap to collect.
    errors: list[BaseException] = []
    errors_lock = threading.Lock()

    def wrapper(*args: object) -> None:
        try:
            target(*args)
        except BaseException as error:
            with errors_lock:
                errors.append(error)

    threads = [threading.Thread(target=wrapper, args=args) for args in args_per_worker]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    if errors:
        raise errors[0]


def test_empty_dir_returns_no_samples(tmp_path: pathlib.Path) -> None:
    with dl_prometheus.MultiprocessMetricsRegistry(metrics_dir=tmp_path, metrics=()) as registry:
        assert registry.get_samples() == []


def test_get_latest_returns_prometheus_text_exposition(tmp_path: pathlib.Path) -> None:
    counter = dl_prometheus.Counter(name="events_total", documentation="event count")
    with dl_prometheus.MultiprocessMetricsRegistry(metrics_dir=tmp_path, metrics=(counter,)) as registry:
        counter.inc(7.0)
        latest = registry.get_latest()

    assert isinstance(latest, dl_prometheus.Latest)
    assert latest.content_type.startswith("text/plain")
    body = latest.body.decode("utf-8")
    assert "events_total 7.0" in body


def test_metrics_dir_is_created_when_missing(tmp_path: pathlib.Path) -> None:
    missing_dir = tmp_path / "deep" / "nested" / "metrics"
    assert not missing_dir.exists()

    with dl_prometheus.MultiprocessMetricsRegistry(metrics_dir=missing_dir, metrics=()):
        assert missing_dir.is_dir()


def test_counter_aggregates_across_processes(tmp_path: pathlib.Path) -> None:
    _run_multiprocess_workers(
        target=_increment_counter,
        args_per_worker=[(tmp_path, [1.0, 2.0]), (tmp_path, [4.0])],
    )

    with dl_prometheus.MultiprocessMetricsRegistry(metrics_dir=tmp_path, metrics=()) as registry:
        assert registry.get_samples() == [
            prometheus_client.samples.Sample(name="events_total", labels={}, value=7.0),
        ]


def test_counter_aggregates_across_threads(tmp_path: pathlib.Path) -> None:
    counter = dl_prometheus.Counter(name="events_total", documentation="events")
    with dl_prometheus.MultiprocessMetricsRegistry(metrics_dir=tmp_path, metrics=(counter,)) as registry:

        def increment(amounts: list[float]) -> None:
            for amount in amounts:
                counter.inc(amount)

        _run_multithread_workers(
            target=increment,
            args_per_worker=[([1.0, 2.0],), ([4.0],)],
        )

        assert registry.get_samples() == [
            prometheus_client.samples.Sample(name="events_total", labels={}, value=7.0),
        ]


def test_counter_with_labels_aggregates_across_processes(tmp_path: pathlib.Path) -> None:
    _run_multiprocess_workers(
        target=_increment_labeled_counter,
        args_per_worker=[
            (tmp_path, "read", 3.0),
            (tmp_path, "write", 5.0),
            (tmp_path, "read", 2.0),
        ],
    )

    with dl_prometheus.MultiprocessMetricsRegistry(metrics_dir=tmp_path, metrics=()) as registry:
        samples_by_label = {tuple(sorted(sample.labels.items())): sample.value for sample in registry.get_samples()}

    assert samples_by_label == {
        (("kind", "read"),): 5.0,
        (("kind", "write"),): 5.0,
    }


def test_gauge_sum_aggregates_across_processes(tmp_path: pathlib.Path) -> None:
    _run_multiprocess_workers(
        target=_set_gauge_sum,
        args_per_worker=[(tmp_path, 3.0), (tmp_path, 5.0)],
    )

    with dl_prometheus.MultiprocessMetricsRegistry(metrics_dir=tmp_path, metrics=()) as registry:
        assert registry.get_samples() == [
            prometheus_client.samples.Sample(name="memory_bytes", labels={}, value=8.0),
        ]


def test_histogram_aggregates_across_processes(tmp_path: pathlib.Path) -> None:
    _run_multiprocess_workers(
        target=_observe_histogram,
        args_per_worker=[(tmp_path, [0.5, 2.0]), (tmp_path, [3.0])],
    )

    with dl_prometheus.MultiprocessMetricsRegistry(metrics_dir=tmp_path, metrics=()) as registry:
        samples_by_key = {
            (sample.name, tuple(sorted(sample.labels.items()))): sample.value for sample in registry.get_samples()
        }

    assert samples_by_key[("latency_seconds_bucket", (("le", "1.0"),))] == 1.0
    assert samples_by_key[("latency_seconds_bucket", (("le", "5.0"),))] == 3.0
    assert samples_by_key[("latency_seconds_bucket", (("le", "+Inf"),))] == 3.0
    assert samples_by_key[("latency_seconds_sum", ())] == 5.5
    assert samples_by_key[("latency_seconds_count", ())] == 3.0


def test_summary_aggregates_across_processes(tmp_path: pathlib.Path) -> None:
    _run_multiprocess_workers(
        target=_observe_summary,
        args_per_worker=[(tmp_path, [0.5, 2.0]), (tmp_path, [3.0])],
    )

    with dl_prometheus.MultiprocessMetricsRegistry(metrics_dir=tmp_path, metrics=()) as registry:
        samples_by_name = {sample.name: sample.value for sample in registry.get_samples()}

    assert samples_by_name["response_time_seconds_sum"] == 5.5
    assert samples_by_name["response_time_seconds_count"] == 3.0


def test_does_not_double_count_when_metrics_are_declared_on_aggregator(tmp_path: pathlib.Path) -> None:
    _run_multiprocess_workers(
        target=_increment_counter,
        args_per_worker=[(tmp_path, [1.0, 2.0]), (tmp_path, [4.0])],
    )

    counter_declaration = dl_prometheus.Counter(name="events_total", documentation="events")
    with dl_prometheus.MultiprocessMetricsRegistry(
        metrics_dir=tmp_path,
        metrics=(counter_declaration,),
    ) as registry:
        assert registry.get_samples() == [
            prometheus_client.samples.Sample(name="events_total", labels={}, value=7.0),
        ]


def test_does_not_double_count_when_metrics_are_declared_on_aggregator_across_threads(
    tmp_path: pathlib.Path,
) -> None:
    worker_counter = dl_prometheus.Counter(name="events_total", documentation="events")
    with dl_prometheus.MultiprocessMetricsRegistry(metrics_dir=tmp_path, metrics=(worker_counter,)):

        def increment(amounts: list[float]) -> None:
            for amount in amounts:
                worker_counter.inc(amount)

        _run_multithread_workers(
            target=increment,
            args_per_worker=[([1.0, 2.0],), ([4.0],)],
        )

        aggregator_counter_declaration = dl_prometheus.Counter(name="events_total", documentation="events")
        with dl_prometheus.MultiprocessMetricsRegistry(
            metrics_dir=tmp_path,
            metrics=(aggregator_counter_declaration,),
        ) as aggregator_registry:
            assert aggregator_registry.get_samples() == [
                prometheus_client.samples.Sample(name="events_total", labels={}, value=7.0),
            ]


def test_raises_on_metrics_dir_mismatch(tmp_path: pathlib.Path) -> None:
    dir_one = tmp_path / "r1"
    dir_two = tmp_path / "r2"
    dir_one.mkdir()
    dir_two.mkdir()

    with dl_prometheus.MultiprocessMetricsRegistry(metrics_dir=dir_one, metrics=()):
        with pytest.raises(ValueError, match="metrics_dir"):
            dl_prometheus.MultiprocessMetricsRegistry(metrics_dir=dir_two, metrics=())


def test_can_recreate_with_different_metrics_dir_after_close(tmp_path: pathlib.Path) -> None:
    dir_one = tmp_path / "r1"
    dir_two = tmp_path / "r2"
    dir_one.mkdir()
    dir_two.mkdir()

    with dl_prometheus.MultiprocessMetricsRegistry(metrics_dir=dir_one, metrics=()):
        pass

    with dl_prometheus.MultiprocessMetricsRegistry(metrics_dir=dir_two, metrics=()):
        pass


def test_constructor_failure_rolls_back_patch(tmp_path: pathlib.Path) -> None:
    # Registering the same metric twice raises inside
    # super().__attrs_post_init__(), AFTER the refcount has been bumped.
    # Without rollback the bump would leak, so the patch wouldn't be
    # restored when the outer registry exits.
    original = prometheus_client.values.ValueClass
    counter = dl_prometheus.Counter(name="dup_total", documentation="dup")
    with dl_prometheus.MultiprocessMetricsRegistry(metrics_dir=tmp_path, metrics=(counter,)):
        with pytest.raises(RuntimeError):
            dl_prometheus.MultiprocessMetricsRegistry(metrics_dir=tmp_path, metrics=(counter,))
    assert prometheus_client.values.ValueClass is original


def test_close_is_idempotent(tmp_path: pathlib.Path) -> None:
    original = prometheus_client.values.ValueClass
    registry = dl_prometheus.MultiprocessMetricsRegistry(metrics_dir=tmp_path, metrics=())
    try:
        assert prometheus_client.values.ValueClass is not original
        registry.close()
        registry.close()
        registry.close()
        assert prometheus_client.values.ValueClass is original
    finally:
        registry.close()


def test_iter_metrics_after_close_raises(tmp_path: pathlib.Path) -> None:
    registry = dl_prometheus.MultiprocessMetricsRegistry(metrics_dir=tmp_path, metrics=())
    registry.close()
    with pytest.raises(RuntimeError, match="closed"):
        registry.get_samples()


def test_value_class_patched_during_registry_lifetime(tmp_path: pathlib.Path) -> None:
    original = prometheus_client.values.ValueClass

    with dl_prometheus.MultiprocessMetricsRegistry(metrics_dir=tmp_path, metrics=()):
        assert prometheus_client.values.ValueClass is not original
    assert prometheus_client.values.ValueClass is original


def test_value_class_patched_once_for_multiple_registries(tmp_path: pathlib.Path) -> None:
    original = prometheus_client.values.ValueClass

    with dl_prometheus.MultiprocessMetricsRegistry(metrics_dir=tmp_path, metrics=()):
        patched = prometheus_client.values.ValueClass
        assert patched is not original

        with dl_prometheus.MultiprocessMetricsRegistry(metrics_dir=tmp_path, metrics=()):
            assert prometheus_client.values.ValueClass is patched
        assert prometheus_client.values.ValueClass is patched

    assert prometheus_client.values.ValueClass is original


def test_value_class_patch_toggles_across_lifecycles(tmp_path: pathlib.Path) -> None:
    original = prometheus_client.values.ValueClass

    for _ in range(3):
        with dl_prometheus.MultiprocessMetricsRegistry(metrics_dir=tmp_path, metrics=()):
            assert prometheus_client.values.ValueClass is not original
        assert prometheus_client.values.ValueClass is original


def test_patched_value_class_handles_pid_change(tmp_path: pathlib.Path) -> None:
    # Simulate fork without actually forking: drive the process_identifier
    # closure return value by hand and verify the value class re-opens its
    # mmap file under the new PID.
    pid_box = {"value": 1000}

    def fake_pid() -> int:
        return pid_box["value"]

    value_class = multiprocess_module.PatchedMultiProcessValue(
        metrics_dir=tmp_path,
        process_identifier=fake_pid,
    )

    value = value_class(
        typ="counter",
        metric_name="events_total",
        name="events_total",
        labelnames=[],
        labelvalues=[],
        help_text="events",
    )
    value.inc(5.0)
    assert value.get() == 5.0
    assert sorted(os.listdir(tmp_path)) == ["counter_1000.db"]

    pid_box["value"] = 2000
    value.inc(3.0)
    # After PID change, the new file starts empty (so _value resets to 0),
    # then inc(3.0) leaves it at 3.0. The old file is untouched on disk.
    assert value.get() == 3.0
    assert sorted(os.listdir(tmp_path)) == ["counter_1000.db", "counter_2000.db"]


def test_construction_multiprocess(tmp_path: pathlib.Path) -> None:
    original = prometheus_client.values.ValueClass

    _run_multiprocess_workers(
        target=_assert_registry_patches_value_class,
        args_per_worker=[(tmp_path,)] * 4,
    )

    assert prometheus_client.values.ValueClass is original


def test_construction_multithreading(tmp_path: pathlib.Path) -> None:
    original = prometheus_client.values.ValueClass
    barrier = threading.Barrier(8)
    registries: list[dl_prometheus.MultiprocessMetricsRegistry] = []
    registries_lock = threading.Lock()

    def worker() -> None:
        registry = dl_prometheus.MultiprocessMetricsRegistry(metrics_dir=tmp_path, metrics=())
        with registries_lock:
            registries.append(registry)
        barrier.wait()
        assert prometheus_client.values.ValueClass is not original

    try:
        _run_multithread_workers(target=worker, args_per_worker=[()] * 8)

        assert len(registries) == 8
        assert prometheus_client.values.ValueClass is not original
    finally:
        for registry in registries:
            registry.close()
    assert prometheus_client.values.ValueClass is original


def test_construct_destruct_multiprocess(tmp_path: pathlib.Path) -> None:
    original = prometheus_client.values.ValueClass

    _run_multiprocess_workers(
        target=_assert_registry_close_restores_value_class,
        args_per_worker=[(tmp_path,)] * 4,
    )

    assert prometheus_client.values.ValueClass is original


def test_construct_destruct_multithreading(tmp_path: pathlib.Path) -> None:
    original = prometheus_client.values.ValueClass

    def worker() -> None:
        with dl_prometheus.MultiprocessMetricsRegistry(metrics_dir=tmp_path, metrics=()):
            pass

    _run_multithread_workers(target=worker, args_per_worker=[()] * 16)

    assert prometheus_client.values.ValueClass is original


def test_keeper_keeps_patch_multiprocess(tmp_path: pathlib.Path) -> None:
    original = prometheus_client.values.ValueClass

    with dl_prometheus.MultiprocessMetricsRegistry(metrics_dir=tmp_path, metrics=()):
        _run_multiprocess_workers(
            target=_assert_registry_close_restores_value_class,
            args_per_worker=[(tmp_path,)] * 4,
        )

        # Workers ran in isolated processes — pytest's keeper still alive, so
        # the patch in this process remains.
        assert prometheus_client.values.ValueClass is not original

    assert prometheus_client.values.ValueClass is original


def test_keeper_keeps_patch_multithreading(tmp_path: pathlib.Path) -> None:
    original = prometheus_client.values.ValueClass

    with dl_prometheus.MultiprocessMetricsRegistry(metrics_dir=tmp_path, metrics=()):
        barrier = threading.Barrier(8)

        def worker() -> None:
            with dl_prometheus.MultiprocessMetricsRegistry(metrics_dir=tmp_path, metrics=()):
                barrier.wait()

        _run_multithread_workers(target=worker, args_per_worker=[()] * 8)

        assert prometheus_client.values.ValueClass is not original

    assert prometheus_client.values.ValueClass is original
