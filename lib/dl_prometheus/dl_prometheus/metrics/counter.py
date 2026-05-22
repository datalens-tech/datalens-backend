from typing import Mapping

import attrs
import prometheus_client

import dl_prometheus.metrics.base as base


@attrs.define(kw_only=True, eq=False, slots=False)
class Counter(base.MetricBase[prometheus_client.Counter]):
    def _build_inner(self) -> prometheus_client.Counter:
        return prometheus_client.Counter(
            name=self._name,
            documentation=self._documentation,
            labelnames=self._labelnames,
            registry=None,
        )

    def inc(self, amount: float = 1.0, *, labels: Mapping[str, str] | None = None) -> None:
        self._select(labels).inc(amount)
