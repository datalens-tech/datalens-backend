import attr

from dl_core.connection_models.dto_defs import ConnDTO

from dl_connector_starrocks.core.constants import CONNECTION_TYPE_STARROCKS


@attr.s(frozen=True, kw_only=True)
class StarRocksConnDTO(ConnDTO):
    conn_type = CONNECTION_TYPE_STARROCKS
    host: str = attr.ib()
    multihosts: tuple[str, ...] = attr.ib()
    port: int = attr.ib()
    username: str = attr.ib()
    password: str = attr.ib(repr=False)

    def get_all_hosts(self) -> list[str]:
        return list(self.multihosts) if self.multihosts else [self.host] if self.host else []

    def conn_reporting_data(self) -> dict:
        return super().conn_reporting_data() | dict(
            host=self.host,
        )
