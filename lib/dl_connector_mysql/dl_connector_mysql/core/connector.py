from dl_core.connectors.base.connector import (
    CoreBackendDefinition,
    CoreConnectionDefinition,
    CoreConnector,
)
from dl_core.connectors.sql_base.connector import (
    SQLSubselectCoreSourceDefinitionBase,
    SQLTableCoreSourceDefinitionBase,
)

from dl_connector_mysql.core.adapters_mysql import MySQLAdapter
from dl_connector_mysql.core.async_adapters_mysql import AsyncMySQLAdapter
from dl_connector_mysql.core.connection_executors import (
    AsyncMySQLConnExecutor,
    MySQLConnExecutor,
)
from dl_connector_mysql.core.constants import (
    BACKEND_TYPE_MYSQL,
    CONNECTION_TYPE_MYSQL,
    SOURCE_TYPE_MYSQL_SUBSELECT,
    SOURCE_TYPE_MYSQL_TABLE,
)
from dl_connector_mysql.core.data_source import (
    MySQLDataSource,
    MySQLSubselectDataSource,
)
from dl_connector_mysql.core.data_source_migration import MySQLDataSourceMigrator
from dl_connector_mysql.core.query_compiler import MySQLQueryCompiler
from dl_connector_mysql.core.sa_types import SQLALCHEMY_MYSQL_TYPES
from dl_connector_mysql.core.storage_schemas.connection import ConnectionMySQLDataStorageSchema
from dl_connector_mysql.core.type_transformer import MySQLTypeTransformer
from dl_connector_mysql.core.us_connection import ConnectionMySQL


class MySQLCoreConnectionDefinition(CoreConnectionDefinition):
    conn_type = CONNECTION_TYPE_MYSQL
    connection_cls = ConnectionMySQL
    us_storage_schema_cls = ConnectionMySQLDataStorageSchema
    type_transformer_cls = MySQLTypeTransformer
    sync_conn_executor_cls = MySQLConnExecutor
    async_conn_executor_cls = AsyncMySQLConnExecutor
    dialect_string = "dl_mysql"
    data_source_migrator_cls = MySQLDataSourceMigrator


class MySQLTableCoreSourceDefinition(SQLTableCoreSourceDefinitionBase):
    source_type = SOURCE_TYPE_MYSQL_TABLE
    source_cls = MySQLDataSource


class MySQLSubselectCoreSourceDefinition(SQLSubselectCoreSourceDefinitionBase):
    source_type = SOURCE_TYPE_MYSQL_SUBSELECT
    source_cls = MySQLSubselectDataSource


class MySQLCoreBackendDefinition(CoreBackendDefinition):
    backend_type = BACKEND_TYPE_MYSQL
    compiler_cls = MySQLQueryCompiler


class MySQLCoreConnector(CoreConnector):
    backend_definition = MySQLCoreBackendDefinition
    connection_definitions = (MySQLCoreConnectionDefinition,)
    source_definitions = (
        MySQLTableCoreSourceDefinition,
        MySQLSubselectCoreSourceDefinition,
    )
    rqe_adapter_classes = frozenset({MySQLAdapter, AsyncMySQLAdapter})
    sa_types = SQLALCHEMY_MYSQL_TYPES
