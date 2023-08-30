from __future__ import annotations

from typing import ClassVar, Optional

from bi_constants.enums import ConnectionType

from bi_core.data_source.sql import PseudoSQLDataSource


class PromQLDataSource(PseudoSQLDataSource):
    preview_enabled: ClassVar[bool] = False
    supports_offset: ClassVar[bool] = False

    conn_type = ConnectionType.promql

    @property
    def db_name(self) -> Optional[str]:
        return ''
