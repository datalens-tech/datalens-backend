from __future__ import annotations

import attr

from dl_core.connection_models.dto_defs import DefaultSQLDTO

from dl_connector_starrocks.core.constants import CONNECTION_TYPE_STARROCKS


@attr.s(frozen=True)
class StarRocksConnDTO(DefaultSQLDTO):
    conn_type = CONNECTION_TYPE_STARROCKS
