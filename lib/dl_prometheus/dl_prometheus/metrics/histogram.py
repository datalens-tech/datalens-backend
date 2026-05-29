from collections.abc import Mapping, Sequence

import attrs
import prometheus_client

import dl_prometheus.metrics.base as base


@attrs.define(kw_only=True, eq=False, slots=False)
class Histogram(base.MetricBase[prometheus_client.Histogram]):
    _buckets: Sequence[float] = prometheus_client.Histogram.DEFAULT_BUCKETS

    def _build_inner(self) -> prometheus_client.Histogram:
        return prometheus_client.Histogram(
            name=self._name,
            documentation=self._documentation,
            labelnames=self._labelnames,
            registry=None,
            buckets=self._buckets,
        )

    def observe(self, amount: float, *, labels: Mapping[str, str] | None = None) -> None:
        self._select(labels).observe(amount)
