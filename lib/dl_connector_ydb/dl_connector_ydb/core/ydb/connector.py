from ydb.sqlalchemy import register_dialect as yql_register_dialect

from dl_core.connectors.base.connector import (
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
from dl_connector_ydb.core.ydb.type_transformer import YDBTypeTransformer
from dl_connector_ydb.core.ydb.us_connection import YDBConnection


class YDBCoreConnectionDefinition(CoreConnectionDefinition):
    conn_type = CONNECTION_TYPE_YDB
    connection_cls = YDBConnection
    us_storage_schema_cls = YDBConnectionDataStorageSchema
    type_transformer_cls = YDBTypeTransformer
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


class YDBCoreConnector(CoreConnector):
    backend_type = BACKEND_TYPE_YDB
    connection_definitions = (YDBCoreConnectionDefinition,)
    source_definitions = (
        YDBCoreSourceDefinition,
        YDBCoreSubselectSourceDefinition,
    )
    rqe_adapter_classes = frozenset({YDBAdapter})
    compiler_cls = YQLQueryCompiler

    @classmethod
    def registration_hook(cls) -> None:
        yql_register_dialect()
