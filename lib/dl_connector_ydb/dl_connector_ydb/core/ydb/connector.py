from dl_core.connectors.base.connector import (
    CoreBackendDefinition,
    CoreConnectionDefinition,
    CoreConnector,
    CoreSourceDefinition,
)
from dl_core.data_source_spec.sql import (
    StandardSQLDataSourceSpec,
    SubselectDataSourceSpec,
)
from dl_core.us_manager.storage_schemas.data_source_spec_base import (
    SQLDataSourceSpecStorageSchema,
    SubselectDataSourceSpecStorageSchema,
)

from dl_connector_ydb.core.base.query_compiler import YQLQueryCompiler
from dl_connector_ydb.core.base.type_transformer import YQLTypeTransformer
from dl_connector_ydb.core.ydb.adapter import YDBAdapter
from dl_connector_ydb.core.ydb.connection_executors import YDBAsyncAdapterConnExecutor
from dl_connector_ydb.core.ydb.constants import (
    BACKEND_TYPE_YDB,
    CONNECTION_TYPE_YDB,
    SOURCE_TYPE_YDB_SUBSELECT,
    SOURCE_TYPE_YDB_TABLE,
)
from dl_connector_ydb.core.ydb.data_source import (
    YDBSubselectDataSource,
    YDBTableDataSource,
)
from dl_connector_ydb.core.ydb.settings import YDBSettingDefinition
from dl_connector_ydb.core.ydb.storage_schemas.connection import YDBConnectionDataStorageSchema
from dl_connector_ydb.core.ydb.us_connection import YDBConnection


class YDBCoreConnectionDefinition(CoreConnectionDefinition):
    conn_type = CONNECTION_TYPE_YDB
    connection_cls = YDBConnection
    us_storage_schema_cls = YDBConnectionDataStorageSchema
    type_transformer_cls = YQLTypeTransformer
    sync_conn_executor_cls = YDBAsyncAdapterConnExecutor
    async_conn_executor_cls = YDBAsyncAdapterConnExecutor
    dialect_string = "yql"
    settings_definition = YDBSettingDefinition


class YDBCoreSourceDefinition(CoreSourceDefinition):
    source_type = SOURCE_TYPE_YDB_TABLE
    source_cls = YDBTableDataSource
    source_spec_cls = StandardSQLDataSourceSpec
    us_storage_schema_cls = SQLDataSourceSpecStorageSchema


class YDBCoreSubselectSourceDefinition(CoreSourceDefinition):
    source_type = SOURCE_TYPE_YDB_SUBSELECT
    source_cls = YDBSubselectDataSource
    source_spec_cls = SubselectDataSourceSpec
    us_storage_schema_cls = SubselectDataSourceSpecStorageSchema


class YDBCoreBackendDefinition(CoreBackendDefinition):
    backend_type = BACKEND_TYPE_YDB
    compiler_cls = YQLQueryCompiler


class YDBCoreConnector(CoreConnector):
    backend_definition = YDBCoreBackendDefinition
    connection_definitions = (YDBCoreConnectionDefinition,)
    source_definitions = (
        YDBCoreSourceDefinition,
        YDBCoreSubselectSourceDefinition,
    )
    rqe_adapter_classes = frozenset({YDBAdapter})
