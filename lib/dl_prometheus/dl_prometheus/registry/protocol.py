from typing import (
    Iterator,
    Protocol,
    runtime_checkable,
)

import prometheus_client.metrics_core
import prometheus_client.samples

import dl_prometheus.registry.base as base


@runtime_checkable
class MetricsRegistryProtocol(Protocol):
    def iter_metrics(self) -> Iterator[prometheus_client.metrics_core.Metric]: ...
    def get_metrics(self) -> list[prometheus_client.metrics_core.Metric]: ...
    def iter_samples(self) -> Iterator[prometheus_client.samples.Sample]: ...
    def get_samples(self) -> list[prometheus_client.samples.Sample]: ...
    def get_latest(self) -> base.Latest: ...
    def close(self) -> None: ...
