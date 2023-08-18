import attr

from bi_core.connection_models.dto_defs import ConnDTO

from bi_connector_metrica.core.constants import CONNECTION_TYPE_METRICA_API, CONNECTION_TYPE_APPMETRICA_API


@attr.s(frozen=True)
class MetricaAPIConnDTO(ConnDTO):  # noqa
    conn_type = CONNECTION_TYPE_METRICA_API

    token: str = attr.ib(repr=False, kw_only=True)
    accuracy: float = attr.ib(kw_only=True)


@attr.s(frozen=True)
class AppMetricaAPIConnDTO(MetricaAPIConnDTO):
    conn_type = CONNECTION_TYPE_APPMETRICA_API
