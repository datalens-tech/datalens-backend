from __future__ import annotations

import attr

from bi_core.connection_models.dto_defs import ConnDTO

from bi_connector_yql.core.yq.constants import CONNECTION_TYPE_YQ


@attr.s(frozen=True)
class YQConnDTO(ConnDTO):
    conn_type = CONNECTION_TYPE_YQ
    service_account_id: str = attr.ib(kw_only=True)
    folder_id: str = attr.ib(kw_only=True)
    host: str = attr.ib(kw_only=True)
    port: int = attr.ib(kw_only=True)
    db_name: str = attr.ib(kw_only=True)
    password: str = attr.ib(repr=False, kw_only=True)
