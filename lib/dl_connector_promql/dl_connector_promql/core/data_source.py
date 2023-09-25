from __future__ import annotations

from typing import (
    ClassVar,
    Optional,
)

from dl_connector_promql.core.constants import CONNECTION_TYPE_PROMQL
from dl_core.data_source.sql import PseudoSQLDataSource


class PromQLDataSource(PseudoSQLDataSource):
    preview_enabled: ClassVar[bool] = False
    supports_offset: ClassVar[bool] = False

    conn_type = CONNECTION_TYPE_PROMQL

    @property
    def db_name(self) -> Optional[str]:
        return ""
