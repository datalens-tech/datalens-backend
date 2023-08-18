from bi_constants.enums import SourceBackendType, ConnectionType, CreateDSFrom

from bi_core.connectors.base.connector import (
    CoreConnector, CoreConnectionDefinition, CoreSourceDefinition,
)
from bi_core.connectors.chydb.us_connection import ConnectionCHYDB
from bi_core.connectors.chydb.storage_schemas.connection import ConnectionCHYDBDataStorageSchema
from bi_core.connectors.chydb.type_transformer import CHYDBTypeTransformer
from bi_core.connectors.chydb.connection_executors import CHYDBSyncAdapterConnExecutor, CHYDBAsyncAdapterConnExecutor
from bi_core.connectors.chydb.data_source import CHYDBTableDataSource, CHYDBSubselectDataSource
from bi_core.connectors.chydb.adapters import CHYDBAdapter
from bi_core.connectors.chydb.async_adapters import AsyncCHYDBAdapter
from bi_core.connectors.chydb.data_source_spec import CHYDBTableDataSourceSpec, CHYDBSubselectDataSourceSpec
from bi_core.connectors.chydb.storage_schemas.data_source_spec import (
    CHYDBTableDataSourceSpecStorageSchema, CHYDBSubselectDataSourceSpecStorageSchema
)


class CHYDBCoreConnectionDefinition(CoreConnectionDefinition):
    conn_type = ConnectionType.chydb
    connection_cls = ConnectionCHYDB
    us_storage_schema_cls = ConnectionCHYDBDataStorageSchema
    type_transformer_cls = CHYDBTypeTransformer
    sync_conn_executor_cls = CHYDBSyncAdapterConnExecutor
    async_conn_executor_cls = CHYDBAsyncAdapterConnExecutor
    dialect_string = 'bi_clickhouse'


class CHYDBCoreSourceDefinition(CoreSourceDefinition):
    source_type = CreateDSFrom.CHYDB_TABLE
    source_cls = CHYDBTableDataSource
    source_spec_cls = CHYDBTableDataSourceSpec
    us_storage_schema_cls = CHYDBTableDataSourceSpecStorageSchema


class CHYDBCoreSubselectSourceDefinition(CoreSourceDefinition):
    source_type = CreateDSFrom.CHYDB_SUBSELECT
    source_cls = CHYDBSubselectDataSource
    source_spec_cls = CHYDBSubselectDataSourceSpec
    us_storage_schema_cls = CHYDBSubselectDataSourceSpecStorageSchema


class CHYDBCoreConnector(CoreConnector):
    backend_type = SourceBackendType.CHYDB
    connection_definitions = (
        CHYDBCoreConnectionDefinition,
    )
    source_definitions = (
        CHYDBCoreSourceDefinition,
        CHYDBCoreSubselectSourceDefinition,
    )
    rqe_adapter_classes = frozenset({CHYDBAdapter, AsyncCHYDBAdapter})
