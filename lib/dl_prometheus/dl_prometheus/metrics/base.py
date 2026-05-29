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
# The inner prometheus_client metric is built lazily on register(), not on
# __attrs_post_init__. This is load-bearing for MultiprocessMetricsRegistry:
# constructing prometheus_client.Counter (etc.) eagerly materializes its
# _value using whatever prometheus_client.values.ValueClass is currently
# installed. If we build the inner during MetricBase.__init__, then the
# value class is captured *before* MultiprocessMetricsRegistry has had a
# chance to install its patched ValueClass — and counter.inc() writes go to
# in-process memory instead of the shared mmap files. Building inside
# register() ensures the patch is in place first (the registry applies the
# patch before delegating to BaseMetricsRegistry.__attrs_post_init__, which
# is where metric.register() is called).

from collections.abc import Mapping
from typing import (
    Generic,
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
    _inner: _InnerMetricT | None = attrs.field(init=False, default=None)

    def _build_inner(self) -> _InnerMetricT:
        raise NotImplementedError

    def register(self, collector_registry: prometheus_client.CollectorRegistry) -> None:
        if self._inner is not None:
            raise RuntimeError(
                f"metric {self._name!r} is already attached to a MetricsRegistry",
            )

        self._inner = self._build_inner()
        collector_registry.register(self._inner)

    def _select(self, labels: Mapping[str, str] | None) -> _InnerMetricT:
        if self._inner is None:
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
