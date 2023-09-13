from __future__ import annotations

from typing import Callable, ClassVar, Optional

import attr

from bi_configs.connectors_settings import MonitoringConnectorSettings
from bi_core.base_models import ConnCacheableDataModelMixin

from bi_core.connection_executors.sync_base import SyncConnExecutorBase
from bi_core.us_connection_base import (
    ConnectionBase,
    SubselectMixin,
    ExecutorBasedMixin,
    ConnectionHardcodedDataMixin,
    DataSourceTemplate,
)
from bi_connector_monitoring.core.dto import MonitoringConnDTO


class MonitoringConnection(
        ConnectionHardcodedDataMixin[MonitoringConnectorSettings],
        SubselectMixin,
        ExecutorBasedMixin,
        ConnectionBase,
):
    allow_cache: ClassVar[bool] = True
    allow_dashsql: ClassVar[bool] = True
    settings_type = MonitoringConnectorSettings

    @attr.s(kw_only=True)
    class DataModel(ConnCacheableDataModelMixin, ConnectionBase.DataModel):
        service_account_id: Optional[str] = attr.ib(default=None)
        folder_id: Optional[str] = attr.ib(default=None)

    def get_conn_dto(self) -> MonitoringConnDTO:
        cs = self._connector_settings
        return MonitoringConnDTO(
            conn_id=self.uuid,
            host=cs.HOST,
            url_path=cs.URL_PATH,
            service_account_id=self.data.service_account_id,
            folder_id=self.data.folder_id,
        )

    @property
    def cache_ttl_sec_override(self) -> Optional[int]:
        return self.data.cache_ttl_sec

    @property
    def is_dashsql_allowed(self) -> bool:
        return True

    @property
    def is_subselect_allowed(self) -> bool:
        return False

    def get_data_source_templates(
            self, conn_executor_factory: Callable[[ConnectionBase], SyncConnExecutorBase]
    ) -> list[DataSourceTemplate]:
        return []

    @property
    def allow_public_usage(self) -> bool:
        return True
