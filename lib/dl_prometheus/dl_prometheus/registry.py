# Why wrap prometheus_client.CollectorRegistry?
#
# 1. Owns the metric set explicitly. Wrappers are passed in at construction
#    and registered into an internal CollectorRegistry; the global
#    prometheus_client.REGISTRY is never touched.
# 2. iter_*/get_* helpers expose the contents as flat lists/iterators of
#    Metric and Sample objects without callers needing to know about the
#    inner CollectorRegistry.

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
class MetricsRegistry:
    _metrics: Sequence[metrics.MetricBase]
    _inner: prometheus_client.CollectorRegistry = attrs.field(
        init=False,
        factory=prometheus_client.CollectorRegistry,
    )

    def __attrs_post_init__(self) -> None:
        for metric in self._metrics:
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
