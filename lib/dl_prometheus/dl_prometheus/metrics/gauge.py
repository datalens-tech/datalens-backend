from typing import Mapping

import attrs
import prometheus_client

import dl_prometheus.metrics.base as base


@attrs.define(kw_only=True, eq=False, slots=False)
class Gauge(base.MetricBase[prometheus_client.Gauge]):
    def _build_inner(self) -> prometheus_client.Gauge:
        return prometheus_client.Gauge(
            name=self._name,
            documentation=self._documentation,
            labelnames=self._labelnames,
            registry=None,
        )

    def inc(self, amount: float = 1.0, *, labels: Mapping[str, str] | None = None) -> None:
        self._select(labels).inc(amount)

    def dec(self, amount: float = 1.0, *, labels: Mapping[str, str] | None = None) -> None:
        self._select(labels).dec(amount)

    def set(self, value: float, *, labels: Mapping[str, str] | None = None) -> None:
        self._select(labels).set(value)
