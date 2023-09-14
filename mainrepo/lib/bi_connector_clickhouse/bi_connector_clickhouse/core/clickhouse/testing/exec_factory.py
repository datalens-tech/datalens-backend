from typing import (
    Any,
    Type,
)

from bi_core.connection_executors.adapters.common_base import CommonBaseDirectAdapter
from bi_core.connection_executors.models.connection_target_dto_base import BaseSQLConnTargetDTO
from bi_core_testing.executors import ExecutorFactoryBase

from bi_connector_clickhouse.core.clickhouse_base.adapters import ClickHouseAdapter
from bi_connector_clickhouse.core.clickhouse_base.target_dto import ClickHouseConnTargetDTO


class ClickHouseExecutorFactory(ExecutorFactoryBase):
    def get_dto_class(self) -> Type[BaseSQLConnTargetDTO]:
        return ClickHouseConnTargetDTO

    def get_dba_class(self) -> Type[CommonBaseDirectAdapter]:
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
            insert_quorum=None,
            insert_quorum_timeout=None,
            disable_value_processing=False,
        )
