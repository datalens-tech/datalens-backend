from __future__ import annotations

from typing import ClassVar

from dl_core.data_source.sql import DataSource

from bi_connector_monitoring.core.constants import CONNECTION_TYPE_MONITORING


class MonitoringDataSource(DataSource):
    preview_enabled: ClassVar[bool] = False
    supports_offset: ClassVar[bool] = False

    conn_type = CONNECTION_TYPE_MONITORING

    @property
    def default_title(self) -> str:
        return ''
