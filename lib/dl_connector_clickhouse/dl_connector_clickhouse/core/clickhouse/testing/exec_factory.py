from typing import Any

from dl_core.connection_executors.adapters.common_base import CommonBaseDirectAdapter
from dl_core.connection_executors.models.connection_target_dto_base import BaseSQLConnTargetDTO
from dl_core_testing.executors import ExecutorFactoryBase
from dl_testing.utils import get_root_certificates

from dl_connector_clickhouse.core.clickhouse_base.adapters import ClickHouseAdapter
from dl_connector_clickhouse.core.clickhouse_base.target_dto import ClickHouseConnTargetDTO


class ClickHouseExecutorFactory(ExecutorFactoryBase):
    def get_dto_class(self) -> type[BaseSQLConnTargetDTO]:
        return ClickHouseConnTargetDTO

    def get_dba_class(self) -> type[CommonBaseDirectAdapter]:
        return ClickHouseAdapter

    def get_dto_kwargs(self) -> dict[str, Any]:
        return dict(
            **super().get_dto_kwargs(),
            protocol="http",
            cluster_name=None,
            endpoint=None,
            connect_timeout=None,
            max_execution_time=None,
            total_timeout=None,
            disable_value_processing=False,
            ca_data=get_root_certificates(),
        )
