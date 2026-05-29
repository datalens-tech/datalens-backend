from collections.abc import Mapping

import attrs
import prometheus_client

import dl_prometheus.metrics.base as base


@attrs.define(kw_only=True, eq=False, slots=False)
class Summary(base.MetricBase[prometheus_client.Summary]):
    def _build_inner(self) -> prometheus_client.Summary:
        return prometheus_client.Summary(
            name=self._name,
            documentation=self._documentation,
            labelnames=self._labelnames,
            registry=None,
        )

    def observe(self, amount: float, *, labels: Mapping[str, str] | None = None) -> None:
        self._select(labels).observe(amount)
