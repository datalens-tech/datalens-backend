from typing import Optional

import attr

from dl_core.connection_models.dto_defs import DefaultSQLDTO

from dl_connector_oracle.core.constants import (
    CONNECTION_TYPE_ORACLE,
    OracleDbNameType,
)


@attr.s(frozen=True)
class OracleConnDTO(DefaultSQLDTO):  # noqa
    conn_type = CONNECTION_TYPE_ORACLE

    db_name_type: OracleDbNameType = attr.ib(kw_only=True)
    ssl_enable: bool = attr.ib(kw_only=True, default=False)
    ssl_ca: Optional[str] = attr.ib(kw_only=True, default=None)
