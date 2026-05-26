import attrs

import dl_prometheus.registry.base as base


@attrs.define(kw_only=True, eq=False, slots=False)
class MetricsRegistry(base.BaseMetricsRegistry):
    pass
