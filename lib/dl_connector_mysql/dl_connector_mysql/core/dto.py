import attr

from dl_core.connection_models.dto_defs import DefaultSQLDTO

from dl_connector_mysql.core.constants import (
    CONNECTION_TYPE_MYSQL,
    MySQLEnforceCollateMode,
)


@attr.s(frozen=True)
class MySQLConnDTO(DefaultSQLDTO):
    conn_type = CONNECTION_TYPE_MYSQL
    enforce_collate: MySQLEnforceCollateMode = attr.ib(kw_only=True, default=MySQLEnforceCollateMode.off)
    ssl_enable: bool = attr.ib(kw_only=True, default=False)
    ssl_ca: str | None = attr.ib(kw_only=True, default=None)
