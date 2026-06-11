from collections.abc import Mapping
import enum

import attrs
import prometheus_client

import dl_prometheus.metrics.base as base


class MultiprocessMode(enum.StrEnum):
    """Aggregation mode for a Gauge under MultiprocessMetricsRegistry.

    Mirrors prometheus_client's `_MULTIPROC_MODES`, minus the deprecated
    wildcard alias `'live*'`. The string values match the upstream wire
    format so that mmap file prefixes (e.g. ``gauge_sum_<pid>.db``) and the
    `MultiProcessCollector` continue to interpret them correctly.
    """

    ALL = "all"
    LIVE_ALL = "liveall"
    LIVE_MAX = "livemax"
    LIVE_MIN = "livemin"
    LIVE_MOST_RECENT = "livemostrecent"
    LIVE_SUM = "livesum"
    MAX = "max"
    MIN = "min"
    MOST_RECENT = "mostrecent"
    SUM = "sum"


@attrs.define(kw_only=True, eq=False, slots=False)
class Gauge(base.MetricBase[prometheus_client.Gauge]):
    _multiprocess_mode: MultiprocessMode = MultiprocessMode.ALL

    def _build_inner(self) -> prometheus_client.Gauge:
        return prometheus_client.Gauge(
            name=self._name,
            documentation=self._documentation,
            labelnames=self._labelnames,
            registry=None,
            multiprocess_mode=self._multiprocess_mode.value,
        )

    def inc(self, amount: float = 1.0, *, labels: Mapping[str, str] | None = None) -> None:
        self._select(labels).inc(amount)

    def dec(self, amount: float = 1.0, *, labels: Mapping[str, str] | None = None) -> None:
        self._select(labels).dec(amount)

    def set(self, value: float, *, labels: Mapping[str, str] | None = None) -> None:
        self._select(labels).set(value)
