from __future__ import annotations

from typing import Optional

import attr

from dl_core.connection_models.dto_defs import DefaultSQLDTO

from dl_connector_ydb.core.ydb.constants import CONNECTION_TYPE_YDB


@attr.s(frozen=True)
class YDBConnDTO(DefaultSQLDTO):
    token: Optional[str] = attr.ib(repr=False, kw_only=True, default=None)
    auth_type: str = attr.ib()

    conn_type = CONNECTION_TYPE_YDB
