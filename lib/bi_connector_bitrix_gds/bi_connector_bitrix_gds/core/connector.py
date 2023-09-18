from dl_core.data_source_spec.sql import StandardSQLDataSourceSpec
from dl_core.us_manager.storage_schemas.data_source_spec_base import SQLDataSourceSpecStorageSchema
from dl_core.connectors.base.connector import (
    CoreConnector, CoreConnectionDefinition, CoreSourceDefinition,
)

from bi_connector_bitrix_gds.core.constants import (
    BACKEND_TYPE_BITRIX_GDS, CONNECTION_TYPE_BITRIX24, SOURCE_TYPE_BITRIX_GDS,
)
from bi_connector_bitrix_gds.core.us_connection import BitrixGDSConnection
from bi_connector_bitrix_gds.core.storage_schemas.connection import BitrixGDSConnectionDataStorageSchema
from bi_connector_bitrix_gds.core.type_transformer import BitrixGDSTypeTransformer
from bi_connector_bitrix_gds.core.connection_executors import BitrixGDSAsyncAdapterConnExecutor
from bi_connector_bitrix_gds.core.data_source import BitrixGDSDataSource
from bi_connector_bitrix_gds.core.adapter import BitrixGDSDefaultAdapter


class BitrixGDSCoreConnectionDefinition(CoreConnectionDefinition):
    conn_type = CONNECTION_TYPE_BITRIX24
    connection_cls = BitrixGDSConnection
    us_storage_schema_cls = BitrixGDSConnectionDataStorageSchema
    type_transformer_cls = BitrixGDSTypeTransformer
    sync_conn_executor_cls = BitrixGDSAsyncAdapterConnExecutor
    async_conn_executor_cls = BitrixGDSAsyncAdapterConnExecutor
    dialect_string = 'bi_bitrix'


class BitrixGDSCoreSourceDefinition(CoreSourceDefinition):
    source_type = SOURCE_TYPE_BITRIX_GDS
    source_cls = BitrixGDSDataSource
    source_spec_cls = StandardSQLDataSourceSpec
    us_storage_schema_cls = SQLDataSourceSpecStorageSchema


class BitrixGDSCoreConnector(CoreConnector):
    backend_type = BACKEND_TYPE_BITRIX_GDS
    connection_definitions = (
        BitrixGDSCoreConnectionDefinition,
    )
    source_definitions = (
        BitrixGDSCoreSourceDefinition,
    )
    rqe_adapter_classes = frozenset({BitrixGDSDefaultAdapter})
