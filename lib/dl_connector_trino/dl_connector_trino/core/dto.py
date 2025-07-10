import attr

from dl_core.connection_models.dto_defs import ConnDTO

from dl_connector_trino.core.constants import (
    CONNECTION_TYPE_TRINO,
    TrinoAuthType,
)


@attr.s(frozen=True, kw_only=True)
class TrinoConnDTOBase(ConnDTO):
    conn_type = CONNECTION_TYPE_TRINO
    host: str = attr.ib()
    port: int | None = attr.ib(default=None)
    username: str | None = attr.ib(default=None)
    auth_type: TrinoAuthType = attr.ib()
    password: str | None = attr.ib(repr=False, default=None)
    jwt: str | None = attr.ib(repr=False, default=None)
    ssl_enable: bool = attr.ib(default=False)
    ssl_ca: str | None = attr.ib(default=None)

    def conn_reporting_data(self) -> dict:
        return super().conn_reporting_data() | dict(
            host=self.host,
            port=self.port,
            auth_type=self.auth_type.value,
        )


@attr.s(frozen=True, kw_only=True)
class TrinoConnDTO(TrinoConnDTOBase):
    port: int = attr.ib()
    username: str = attr.ib()
