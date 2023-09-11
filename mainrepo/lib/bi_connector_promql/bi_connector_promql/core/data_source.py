from __future__ import annotations

from typing import ClassVar, Optional

from bi_core.data_source.sql import PseudoSQLDataSource

from bi_connector_promql.core.constants import CONNECTION_TYPE_PROMQL


class PromQLDataSource(PseudoSQLDataSource):
    preview_enabled: ClassVar[bool] = False
    supports_offset: ClassVar[bool] = False

    conn_type = CONNECTION_TYPE_PROMQL

    @property
    def db_name(self) -> Optional[str]:
        return ''
