from typing import Optional

import attr

from dl_core.connection_models.dto_defs import DefaultSQLDTO

from dl_connector_mysql.core.constants import CONNECTION_TYPE_MYSQL


@attr.s(frozen=True)
class MySQLConnDTO(DefaultSQLDTO):
    conn_type = CONNECTION_TYPE_MYSQL
    ssl_enable: bool = attr.ib(kw_only=True, default=False)
    ssl_ca: Optional[str] = attr.ib(kw_only=True, default=None)
