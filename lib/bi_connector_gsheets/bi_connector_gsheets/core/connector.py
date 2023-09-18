from dl_constants.enums import ConnectionType

from dl_core.data_source_spec.sql import SQLDataSourceSpecBase
from dl_core.us_manager.storage_schemas.data_source_spec_base import BaseSQLDataSourceSpecStorageSchema
from dl_core.connectors.base.connector import (
    CoreConnector, CoreConnectionDefinition, CoreSourceDefinition,
)

from bi_connector_gsheets.core.constants import (
    BACKEND_TYPE_GSHEETS,
    CONNECTION_TYPE_GSHEETS,
    SOURCE_TYPE_GSHEETS,
)
from bi_connector_gsheets.core.us_connection import GSheetsConnection
from bi_connector_gsheets.core.storage_schemas.connection import GSheetsConnectionDataStorageSchema
from bi_connector_gsheets.core.type_transformer import GSheetsTypeTransformer
from bi_connector_gsheets.core.connection_executors import GSheetsAsyncAdapterConnExecutor
from bi_connector_gsheets.core.data_source import GSheetsDataSource
from bi_connector_gsheets.core.adapter import GSheetsDefaultAdapter


class GSheetsCoreConnectionDefinition(CoreConnectionDefinition):
    conn_type = CONNECTION_TYPE_GSHEETS
    connection_cls = GSheetsConnection
    us_storage_schema_cls = GSheetsConnectionDataStorageSchema
    type_transformer_cls = GSheetsTypeTransformer
    sync_conn_executor_cls = GSheetsAsyncAdapterConnExecutor
    async_conn_executor_cls = GSheetsAsyncAdapterConnExecutor
    dialect_string = 'gsheets'


class GSheetsCoreSourceDefinition(CoreSourceDefinition):
    source_type = SOURCE_TYPE_GSHEETS
    source_cls = GSheetsDataSource
    source_spec_cls = SQLDataSourceSpecBase
    us_storage_schema_cls = BaseSQLDataSourceSpecStorageSchema


class GSheetsCoreConnector(CoreConnector):
    backend_type = BACKEND_TYPE_GSHEETS
    connection_definitions = (
        GSheetsCoreConnectionDefinition,
    )
    source_definitions = (
        GSheetsCoreSourceDefinition,
    )
    rqe_adapter_classes = frozenset({GSheetsDefaultAdapter})
