from dl_core.connectors.base.connector import (
    CoreBackendDefinition,
    CoreConnectionDefinition,
    CoreConnector,
)
from dl_core.connectors.sql_base.connector import (
    SQLSubselectCoreSourceDefinitionBase,
    SQLTableCoreSourceDefinitionBase,
)

from dl_connector_postgresql.core.postgresql.constants import (
    BACKEND_TYPE_POSTGRES,
    CONNECTION_TYPE_POSTGRES,
    SOURCE_TYPE_PG_SUBSELECT,
    SOURCE_TYPE_PG_TABLE,
)
from dl_connector_postgresql.core.postgresql.data_source import (
    PostgreSQLDataSource,
    PostgreSQLSubselectDataSource,
)
from dl_connector_postgresql.core.postgresql.data_source_migration import PostgreSQLDataSourceMigrator
from dl_connector_postgresql.core.postgresql.settings import PostgreSQLSettingDefinition
from dl_connector_postgresql.core.postgresql.storage_schemas.connection import ConnectionPostgreSQLDataStorageSchema
from dl_connector_postgresql.core.postgresql.us_connection import ConnectionPostgreSQL
from dl_connector_postgresql.core.postgresql_base.adapters_postgres import PostgresAdapter
from dl_connector_postgresql.core.postgresql_base.async_adapters_postgres import AsyncPostgresAdapter
from dl_connector_postgresql.core.postgresql_base.connection_executors import (
    AsyncPostgresConnExecutor,
    PostgresConnExecutor,
)
from dl_connector_postgresql.core.postgresql_base.query_compiler import PostgreSQLQueryCompiler
from dl_connector_postgresql.core.postgresql_base.sa_types import SQLALCHEMY_POSTGRES_TYPES
from dl_connector_postgresql.core.postgresql_base.type_transformer import PostgreSQLTypeTransformer


class PostgreSQLCoreConnectionDefinition(CoreConnectionDefinition):
    conn_type = CONNECTION_TYPE_POSTGRES
    connection_cls = ConnectionPostgreSQL
    us_storage_schema_cls = ConnectionPostgreSQLDataStorageSchema
    type_transformer_cls = PostgreSQLTypeTransformer
    sync_conn_executor_cls = PostgresConnExecutor
    async_conn_executor_cls = AsyncPostgresConnExecutor
    dialect_string = "bi_postgresql"
    data_source_migrator_cls = PostgreSQLDataSourceMigrator
    settings_definition = PostgreSQLSettingDefinition


class PostgreSQLTableCoreSourceDefinition(SQLTableCoreSourceDefinitionBase):
    source_type = SOURCE_TYPE_PG_TABLE
    source_cls = PostgreSQLDataSource


class PostgreSQLSubselectCoreSourceDefinition(SQLSubselectCoreSourceDefinitionBase):
    source_type = SOURCE_TYPE_PG_SUBSELECT
    source_cls = PostgreSQLSubselectDataSource


class PostgreSQLCoreBackendDefinition(CoreBackendDefinition):
    backend_type = BACKEND_TYPE_POSTGRES
    compiler_cls = PostgreSQLQueryCompiler


class PostgreSQLCoreConnector(CoreConnector):
    backend_definition = PostgreSQLCoreBackendDefinition
    connection_definitions = (PostgreSQLCoreConnectionDefinition,)
    source_definitions = (
        PostgreSQLTableCoreSourceDefinition,
        PostgreSQLSubselectCoreSourceDefinition,
    )
    rqe_adapter_classes = frozenset({PostgresAdapter, AsyncPostgresAdapter})
    sa_types = SQLALCHEMY_POSTGRES_TYPES
