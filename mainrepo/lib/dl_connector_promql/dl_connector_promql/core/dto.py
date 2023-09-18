from __future__ import annotations

import attr

from dl_connector_promql.core.constants import CONNECTION_TYPE_PROMQL
from dl_core.connection_models.dto_defs import DefaultSQLDTO


@attr.s(frozen=True)
class PromQLConnDTO(DefaultSQLDTO):
    conn_type = CONNECTION_TYPE_PROMQL

    protocol: str = attr.ib(kw_only=True)
