import pyodbc

from dl_core.connectors.base.connector import (
    CoreBackendDefinition,
    CoreConnectionDefinition,
    CoreConnector,
)
from dl_core.connectors.sql_base.connector import (
    SQLSubselectCoreSourceDefinitionBase,
    SQLTableCoreSourceDefinitionBase,
)

from dl_connector_mssql.core.adapters_mssql import MSSQLDefaultAdapter
from dl_connector_mssql.core.connection_executors import MSSQLConnExecutor
from dl_connector_mssql.core.constants import (
    BACKEND_TYPE_MSSQL,
    CONNECTION_TYPE_MSSQL,
    SOURCE_TYPE_MSSQL_SUBSELECT,
    SOURCE_TYPE_MSSQL_TABLE,
)
from dl_connector_mssql.core.data_source import (
    MSSQLDataSource,
    MSSQLSubselectDataSource,
)
from dl_connector_mssql.core.data_source_migration import MSSQLDataSourceMigrator
from dl_connector_mssql.core.query_compiler import MSSQLQueryCompiler
from dl_connector_mssql.core.sa_types import SQLALCHEMY_MSSQL_TYPES
from dl_connector_mssql.core.storage_schemas.connection import ConnectionMSSQLDataStorageSchema
from dl_connector_mssql.core.type_transformer import MSSQLServerTypeTransformer
from dl_connector_mssql.core.us_connection import ConnectionMSSQL


class MSSQLCoreConnectionDefinition(CoreConnectionDefinition):
    conn_type = CONNECTION_TYPE_MSSQL
    connection_cls = ConnectionMSSQL
    us_storage_schema_cls = ConnectionMSSQLDataStorageSchema
    type_transformer_cls = MSSQLServerTypeTransformer
    sync_conn_executor_cls = MSSQLConnExecutor
    async_conn_executor_cls = MSSQLConnExecutor
    dialect_string = "bi_mssql"
    data_source_migrator_cls = MSSQLDataSourceMigrator


class MSSQLTableCoreSourceDefinition(SQLTableCoreSourceDefinitionBase):
    source_type = SOURCE_TYPE_MSSQL_TABLE
    source_cls = MSSQLDataSource


class MSSQLSubselectCoreSourceDefinition(SQLSubselectCoreSourceDefinitionBase):
    source_type = SOURCE_TYPE_MSSQL_SUBSELECT
    source_cls = MSSQLSubselectDataSource


class MSSQLCoreBackendDefinition(CoreBackendDefinition):
    backend_type = BACKEND_TYPE_MSSQL
    compiler_cls = MSSQLQueryCompiler


class MSSQLCoreConnector(CoreConnector):
    backend_definition = MSSQLCoreBackendDefinition
    connection_definitions = (MSSQLCoreConnectionDefinition,)
    source_definitions = (
        MSSQLTableCoreSourceDefinition,
        MSSQLSubselectCoreSourceDefinition,
    )
    rqe_adapter_classes = frozenset({MSSQLDefaultAdapter})
    sa_types = SQLALCHEMY_MSSQL_TYPES
    query_fail_exceptions = frozenset({pyodbc.Error})
