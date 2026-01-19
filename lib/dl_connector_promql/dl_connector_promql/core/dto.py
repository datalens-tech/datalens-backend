from __future__ import annotations

import attr

from dl_core.connection_models.dto_defs import DefaultSQLDTO

from dl_connector_promql.core.constants import (
    CONNECTION_TYPE_PROMQL,
    PromQLAuthType,
)


@attr.s(frozen=True)
class PromQLConnDTO(DefaultSQLDTO):
    conn_type = CONNECTION_TYPE_PROMQL

    path: str = attr.ib(kw_only=True)
    protocol: str = attr.ib(kw_only=True)
    auth_type: PromQLAuthType = attr.ib(kw_only=True)
    auth_header: str | None = attr.ib(kw_only=True, repr=False)
