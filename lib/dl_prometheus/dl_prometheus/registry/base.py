from typing import (
    Iterator,
    Sequence,
)

import attrs
import prometheus_client
import prometheus_client.metrics_core
import prometheus_client.samples

import dl_prometheus.metrics as metrics


@attrs.define(kw_only=True, eq=False, slots=False)
class BaseMetricsRegistry:
    _metrics: Sequence[metrics.MetricBase]
    _inner: prometheus_client.CollectorRegistry = attrs.field(init=False)

    def __attrs_post_init__(self) -> None:
        self._inner = prometheus_client.CollectorRegistry()
        for metric in self._metrics:
            self.register_metric(metric)

    def register_metric(self, metric: metrics.MetricBase) -> None:
        metric.register(self._inner)

    def iter_metrics(self) -> Iterator[prometheus_client.metrics_core.Metric]:
        yield from self._inner.collect()

    def get_metrics(self) -> list[prometheus_client.metrics_core.Metric]:
        return list(self.iter_metrics())

    def iter_samples(self) -> Iterator[prometheus_client.samples.Sample]:
        for metric in self.iter_metrics():
            yield from metric.samples

    def get_samples(self) -> list[prometheus_client.samples.Sample]:
        return list(self.iter_samples())
