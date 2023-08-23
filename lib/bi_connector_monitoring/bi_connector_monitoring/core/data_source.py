from __future__ import annotations

from typing import ClassVar

from bi_constants.enums import ConnectionType
from bi_core.data_source.sql import DataSource


class MonitoringDataSource(DataSource):
    preview_enabled: ClassVar[bool] = False
    supports_offset: ClassVar[bool] = False

    conn_type = ConnectionType.monitoring

    @property
    def default_title(self) -> str:
        return ''
