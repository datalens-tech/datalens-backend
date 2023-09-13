import attr

from bi_connector_clickhouse.core.clickhouse_base.dto import ClickHouseBaseDTO

from bi_connector_chyt.core.constants import CONNECTION_TYPE_CHYT


@attr.s(frozen=True, kw_only=True)
class BaseCHYTDTO(ClickHouseBaseDTO):
    clique_alias: str = attr.ib(kw_only=True)


@attr.s(frozen=True, kw_only=True)
class CHYTDTO(BaseCHYTDTO):
    conn_type = CONNECTION_TYPE_CHYT

    port: int = attr.ib()
    host: str = attr.ib()
    protocol: str = attr.ib()
    token: str = attr.ib(repr=False)
