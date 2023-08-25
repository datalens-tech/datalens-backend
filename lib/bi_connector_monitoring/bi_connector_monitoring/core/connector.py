from bi_constants.enums import ConnectionType

from bi_core.connectors.base.connector import (
    CoreConnectionDefinition,
    CoreConnector,
    CoreSourceDefinition,
)

from bi_connector_monitoring.core.constants import BACKEND_TYPE_MONITORING, SOURCE_TYPE_MONITORING
from bi_connector_monitoring.core.adapter import AsyncMonitoringAdapter
from bi_connector_monitoring.core.storage_schemas.connection import MonitoringConnectionDataStorageSchema
from bi_connector_monitoring.core.type_transformer import MonitoringTypeTransformer
from bi_connector_monitoring.core.us_connection import MonitoringConnection
from bi_connector_monitoring.core.dto import MonitoringConnDTO
from bi_connector_monitoring.core.connection_executors import MonitoringAsyncAdapterConnExecutor
from bi_connector_monitoring.core.data_source import MonitoringDataSource
from bi_connector_monitoring.core.settings import MonitoringSettingDefinition


class MonitoringCoreConnectionDefinition(CoreConnectionDefinition):
    conn_type = ConnectionType.monitoring
    connection_cls = MonitoringConnection
    us_storage_schema_cls = MonitoringConnectionDataStorageSchema
    type_transformer_cls = MonitoringTypeTransformer
    sync_conn_executor_cls = MonitoringAsyncAdapterConnExecutor
    async_conn_executor_cls = MonitoringAsyncAdapterConnExecutor
    dialect_string = 'bi_solomon'
    settings_definition = MonitoringSettingDefinition


class MonitoringCoreSourceDefinition(CoreSourceDefinition):
    source_type = SOURCE_TYPE_MONITORING
    source_cls = MonitoringDataSource


class MonitoringCoreConnector(CoreConnector):
    backend_type = BACKEND_TYPE_MONITORING
    connection_definitions = (
        MonitoringCoreConnectionDefinition,
    )
    source_definitions = (
        MonitoringCoreSourceDefinition,
    )
    rqe_adapter_classes = frozenset({AsyncMonitoringAdapter})
    safe_dto_classes = frozenset({MonitoringConnDTO})
