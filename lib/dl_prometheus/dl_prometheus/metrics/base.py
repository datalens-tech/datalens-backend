# Why wrap prometheus_client.{Counter,Gauge,Histogram,Summary}?
#
# 1. No accidental leak into prometheus_client.REGISTRY. The default
#    `registry=` arg on prometheus_client metrics is the global REGISTRY;
#    forgetting `registry=None` silently auto-registers into process-global
#    state we explicitly do not want to use. The wrappers never expose
#    `registry=` to the caller and always pass `registry=None` internally.
#
# 2. Use-before-register is a loud error. A wrapper not yet attached to a
#    MetricsRegistry has `_is_registered = False`; any .inc()/.observe()
#    call raises RuntimeError, instead of silently mutating a metric that
#    will never appear in /metrics.
#
# 3. Atomic labels API. counter.inc(amount, labels={...}) is a single
#    validated call; the wrapper checks the label set against the declared
#    labelnames.
#
# Construction model:
# The inner prometheus_client metric is built in MetricBase.__attrs_post_init__
# via _build_inner(), which subclasses override. post_init runs after all
# attrs fields (parent and subclass) are initialized, so subclass-specific
# fields (e.g. Histogram._buckets) are visible.

from typing import (
    Generic,
    Mapping,
    TypeVar,
)

import attrs
import prometheus_client
import prometheus_client.metrics


_InnerMetricT = TypeVar("_InnerMetricT", bound=prometheus_client.metrics.MetricWrapperBase)


@attrs.define(kw_only=True, eq=False, slots=False)
class MetricBase(Generic[_InnerMetricT]):
    _name: str
    _documentation: str
    _labelnames: tuple[str, ...] = ()
    _is_registered: bool = attrs.field(init=False, default=False)
    _inner: _InnerMetricT = attrs.field(init=False)

    def __attrs_post_init__(self) -> None:
        self._inner = self._build_inner()

    def _build_inner(self) -> _InnerMetricT:
        raise NotImplementedError

    def register(self, collector_registry: prometheus_client.CollectorRegistry) -> None:
        if self._is_registered:
            raise RuntimeError(
                f"metric {self._name!r} is already attached to a MetricsRegistry",
            )

        collector_registry.register(self._inner)
        self._is_registered = True

    def _select(self, labels: Mapping[str, str] | None) -> _InnerMetricT:
        if not self._is_registered:
            raise RuntimeError(
                f"metric {self._name!r} is not registered with a MetricsRegistry; "
                f"pass it via MetricsRegistry(metrics=...) before use",
            )

        if not self._labelnames:
            if labels:
                raise ValueError(
                    f"metric {self._name!r} has no labelnames, got labels={dict(labels)!r}",
                )
            return self._inner

        if labels is None:
            raise ValueError(
                f"metric {self._name!r} requires labels {self._labelnames!r}, got None",
            )
        if sorted(labels) != sorted(self._labelnames):
            raise ValueError(
                f"metric {self._name!r} labels mismatch: expected {self._labelnames!r}, " f"got {sorted(labels)!r}",
            )
        return self._inner.labels(**labels)
