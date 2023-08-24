from bi_configs.connectors_settings import CHYTConnectorSettings
from bi_constants.enums import SourceBackendType, ConnectionType, CreateDSFrom

from bi_core.connectors.base.connector import (
    CoreConnector, CoreConnectionDefinition, CoreSourceDefinition,
)
from bi_connector_chyt.core.us_connection import ConnectionCHYTToken
from bi_connector_chyt.core.storage_schemas.connection import (
    ConnectionCHYTDataStorageSchema,
)
from bi_connector_chyt.core.type_transformer import CHYTTypeTransformer
from bi_connector_chyt.core.connection_executors import (
    CHYTSyncAdapterConnExecutor,
    CHYTAsyncAdapterConnExecutor,
)
from bi_connector_chyt.core.data_source import (
    CHYTTableDataSource,
    CHYTTableListDataSource,
    CHYTTableRangeDataSource,
    CHYTTableSubselectDataSource,
)
from bi_connector_chyt.core.adapters import CHYTAdapter
from bi_connector_chyt.core.async_adapters import AsyncCHYTAdapter
from bi_connector_chyt.core.data_source_spec import (
    CHYTTableDataSourceSpec,
    CHYTTableListDataSourceSpec,
    CHYTTableRangeDataSourceSpec,
    CHYTSubselectDataSourceSpec,
)
from bi_connector_chyt.core.storage_schemas.data_source_spec import (
    CHYTTableDataSourceSpecStorageSchema,
    CHYTTableListDataSourceSpecStorageSchema,
    CHYTTableRangeDataSourceSpecStorageSchema,
    CHYTSubselectDataSourceSpecStorageSchema,
)


class CHYTCoreConnectionDefinition(CoreConnectionDefinition):
    conn_type = ConnectionType.chyt
    connection_cls = ConnectionCHYTToken
    us_storage_schema_cls = ConnectionCHYTDataStorageSchema
    type_transformer_cls = CHYTTypeTransformer
    sync_conn_executor_cls = CHYTSyncAdapterConnExecutor
    async_conn_executor_cls = CHYTAsyncAdapterConnExecutor
    dialect_string = 'bi_chyt'
    settings_class = CHYTConnectorSettings


class CHYTTableCoreSourceDefinition(CoreSourceDefinition):
    source_type = CreateDSFrom.CHYT_YTSAURUS_TABLE
    source_cls = CHYTTableDataSource
    source_spec_cls = CHYTTableDataSourceSpec
    us_storage_schema_cls = CHYTTableDataSourceSpecStorageSchema


class CHYTTableListCoreSourceDefinition(CoreSourceDefinition):
    source_type = CreateDSFrom.CHYT_YTSAURUS_TABLE_LIST
    source_cls = CHYTTableListDataSource
    source_spec_cls = CHYTTableListDataSourceSpec
    us_storage_schema_cls = CHYTTableListDataSourceSpecStorageSchema


class CHYTTableRangeCoreSourceDefinition(CoreSourceDefinition):
    source_type = CreateDSFrom.CHYT_YTSAURUS_TABLE_RANGE
    source_cls = CHYTTableRangeDataSource
    source_spec_cls = CHYTTableRangeDataSourceSpec
    us_storage_schema_cls = CHYTTableRangeDataSourceSpecStorageSchema


class CHYTTableSubselectCoreSourceDefinition(CoreSourceDefinition):
    source_type = CreateDSFrom.CHYT_YTSAURUS_SUBSELECT
    source_cls = CHYTTableSubselectDataSource
    source_spec_cls = CHYTSubselectDataSourceSpec
    us_storage_schema_cls = CHYTSubselectDataSourceSpecStorageSchema


class CHYTCoreConnector(CoreConnector):
    backend_type = SourceBackendType.CHYT
    connection_definitions = (
        CHYTCoreConnectionDefinition,
    )
    source_definitions = (
        CHYTTableCoreSourceDefinition,
        CHYTTableListCoreSourceDefinition,
        CHYTTableRangeCoreSourceDefinition,
        CHYTTableSubselectCoreSourceDefinition,
    )
    rqe_adapter_classes = frozenset({
        CHYTAdapter,
        AsyncCHYTAdapter,
    })
