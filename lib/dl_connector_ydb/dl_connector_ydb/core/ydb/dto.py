from __future__ import annotations

import attr

from dl_core.connection_models.dto_defs import DefaultSQLDTO

from dl_connector_ydb.core.ydb.constants import (
    CONNECTION_TYPE_YDB,
    YDBAuthTypeMode,
)


@attr.s(frozen=True)
class YDBConnDTO(DefaultSQLDTO):
    auth_type: YDBAuthTypeMode = attr.ib()

    conn_type = CONNECTION_TYPE_YDB

    ssl_enable: bool = attr.ib(kw_only=True, default=False)
    ssl_ca: str | None = attr.ib(kw_only=True, default=None)
