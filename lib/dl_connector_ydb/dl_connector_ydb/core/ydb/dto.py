from __future__ import annotations

import attr

from dl_core.connection_models.dto_defs import DefaultSQLDTO

from dl_connector_ydb.core.ydb.constants import CONNECTION_TYPE_YDB


@attr.s(frozen=True)
class YDBConnDTO(DefaultSQLDTO):
    conn_type = CONNECTION_TYPE_YDB
