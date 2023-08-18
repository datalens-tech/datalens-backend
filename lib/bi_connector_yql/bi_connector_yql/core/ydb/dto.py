from __future__ import annotations

import attr

from bi_core.connection_models.dto_defs import DefaultSQLDTO

from bi_connector_yql.core.ydb.constants import CONNECTION_TYPE_YDB


@attr.s(frozen=True)
class YDBConnDTO(DefaultSQLDTO):
    conn_type = CONNECTION_TYPE_YDB
    service_account_id: str = attr.ib(kw_only=True)
    folder_id: str = attr.ib(kw_only=True)
