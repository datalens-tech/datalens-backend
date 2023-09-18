from dl_core.connectors.base.connector import CoreConnectionDefinition, CoreConnector
from dl_core.connectors.sql_base.connector import (
    SQLTableCoreSourceDefinitionBase,
    SQLSubselectCoreSourceDefinitionBase,
)

from bi_connector_mysql.core.constants import (
    BACKEND_TYPE_MYSQL, CONNECTION_TYPE_MYSQL,
    SOURCE_TYPE_MYSQL_TABLE, SOURCE_TYPE_MYSQL_SUBSELECT,
)
from bi_connector_mysql.core.adapters_mysql import MySQLAdapter
from bi_connector_mysql.core.async_adapters_mysql import AsyncMySQLAdapter
from bi_connector_mysql.core.type_transformer import MySQLTypeTransformer
from bi_connector_mysql.core.us_connection import ConnectionMySQL
from bi_connector_mysql.core.storage_schemas.connection import ConnectionMySQLDataStorageSchema
from bi_connector_mysql.core.data_source import MySQLDataSource, MySQLSubselectDataSource
from bi_connector_mysql.core.connection_executors import MySQLConnExecutor, AsyncMySQLConnExecutor
from bi_connector_mysql.core.sa_types import SQLALCHEMY_MYSQL_TYPES
from bi_connector_mysql.core.data_source_migration import MySQLDataSourceMigrator


class MySQLCoreConnectionDefinition(CoreConnectionDefinition):
    conn_type = CONNECTION_TYPE_MYSQL
    connection_cls = ConnectionMySQL
    us_storage_schema_cls = ConnectionMySQLDataStorageSchema
    type_transformer_cls = MySQLTypeTransformer
    sync_conn_executor_cls = MySQLConnExecutor
    async_conn_executor_cls = AsyncMySQLConnExecutor
    dialect_string = 'bi_mysql'
    data_source_migrator_cls = MySQLDataSourceMigrator


class MySQLTableCoreSourceDefinition(SQLTableCoreSourceDefinitionBase):
    source_type = SOURCE_TYPE_MYSQL_TABLE
    source_cls = MySQLDataSource


class MySQLSubselectCoreSourceDefinition(SQLSubselectCoreSourceDefinitionBase):
    source_type = SOURCE_TYPE_MYSQL_SUBSELECT
    source_cls = MySQLSubselectDataSource


class MySQLCoreConnector(CoreConnector):
    backend_type = BACKEND_TYPE_MYSQL
    connection_definitions = (
        MySQLCoreConnectionDefinition,
    )
    source_definitions = (
        MySQLTableCoreSourceDefinition,
        MySQLSubselectCoreSourceDefinition,
    )
    rqe_adapter_classes = frozenset({MySQLAdapter, AsyncMySQLAdapter})
    sa_types = SQLALCHEMY_MYSQL_TYPES
