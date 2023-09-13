from bi_core.connectors.base.connector import CoreConnectionDefinition, CoreConnector
from bi_core.connectors.sql_base.connector import (
    SQLTableCoreSourceDefinitionBase,
    SQLSubselectCoreSourceDefinitionBase,
)

from bi_connector_postgresql.core.postgresql.constants import (
    BACKEND_TYPE_POSTGRES, CONNECTION_TYPE_POSTGRES,
    SOURCE_TYPE_PG_TABLE, SOURCE_TYPE_PG_SUBSELECT,
)
from bi_connector_postgresql.core.postgresql_base.adapters_postgres import PostgresAdapter
from bi_connector_postgresql.core.postgresql_base.async_adapters_postgres import AsyncPostgresAdapter
from bi_connector_postgresql.core.postgresql_base.type_transformer import PostgreSQLTypeTransformer
from bi_connector_postgresql.core.postgresql.us_connection import ConnectionPostgreSQL
from bi_connector_postgresql.core.postgresql.storage_schemas.connection import ConnectionPostgreSQLDataStorageSchema
from bi_connector_postgresql.core.postgresql.data_source import PostgreSQLDataSource, PostgreSQLSubselectDataSource
from bi_connector_postgresql.core.postgresql_base.connection_executors import (
    PostgresConnExecutor, AsyncPostgresConnExecutor,
)
from bi_connector_postgresql.core.postgresql_base.sa_types import SQLALCHEMY_POSTGRES_TYPES
from bi_connector_postgresql.core.postgresql.data_source_migration import PostgreSQLDataSourceMigrator


class PostgreSQLCoreConnectionDefinition(CoreConnectionDefinition):
    conn_type = CONNECTION_TYPE_POSTGRES
    connection_cls = ConnectionPostgreSQL
    us_storage_schema_cls = ConnectionPostgreSQLDataStorageSchema
    type_transformer_cls = PostgreSQLTypeTransformer
    sync_conn_executor_cls = PostgresConnExecutor
    async_conn_executor_cls = AsyncPostgresConnExecutor
    dialect_string = 'bi_postgresql'
    data_source_migrator_cls = PostgreSQLDataSourceMigrator


class PostgreSQLTableCoreSourceDefinition(SQLTableCoreSourceDefinitionBase):
    source_type = SOURCE_TYPE_PG_TABLE
    source_cls = PostgreSQLDataSource


class PostgreSQLSubselectCoreSourceDefinition(SQLSubselectCoreSourceDefinitionBase):
    source_type = SOURCE_TYPE_PG_SUBSELECT
    source_cls = PostgreSQLSubselectDataSource


class PostgreSQLCoreConnector(CoreConnector):
    backend_type = BACKEND_TYPE_POSTGRES
    connection_definitions = (
        PostgreSQLCoreConnectionDefinition,
    )
    source_definitions = (
        PostgreSQLTableCoreSourceDefinition,
        PostgreSQLSubselectCoreSourceDefinition,
    )
    rqe_adapter_classes = frozenset({PostgresAdapter, AsyncPostgresAdapter})
    sa_types = SQLALCHEMY_POSTGRES_TYPES
