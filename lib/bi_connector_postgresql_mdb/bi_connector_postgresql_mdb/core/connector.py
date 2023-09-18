from dl_core.connections_security.base import ConnSecuritySettings
from bi_api_lib_ya.connections_security.base import MDBConnectionSafetyChecker
from dl_connector_postgresql.core.postgresql.connector import (
    PostgreSQLCoreConnectionDefinition,
    PostgreSQLCoreConnector,
    PostgreSQLTableCoreSourceDefinition,
    PostgreSQLSubselectCoreSourceDefinition,
)
from dl_connector_postgresql.core.postgresql.dto import PostgresConnDTO

from bi_connector_postgresql_mdb.core.data_source import PostgreSQLMDBDataSource, PostgreSQLMDBSubselectDataSource
from bi_connector_postgresql_mdb.core.us_connection import ConnectionPostgreSQLMDB
from bi_connector_postgresql_mdb.core.storage_schemas import ConnectionPostgreSQLMDBDataStorageSchema
from bi_connector_postgresql_mdb.core.settings import PostgreSQLMDBSettingDefinition
from bi_connector_postgresql_mdb.core.connection_executors import PostgresMDBConnExecutor, AsyncPostgresMDBConnExecutor


class PostgreSQLMDBCoreConnectionDefinition(PostgreSQLCoreConnectionDefinition):
    connection_cls = ConnectionPostgreSQLMDB
    us_storage_schema_cls = ConnectionPostgreSQLMDBDataStorageSchema
    settings_definition = PostgreSQLMDBSettingDefinition
    sync_conn_executor_cls = PostgresMDBConnExecutor
    async_conn_executor_cls = AsyncPostgresMDBConnExecutor


class PostgreSQLMDBTableCoreSourceDefinition(PostgreSQLTableCoreSourceDefinition):
    source_cls = PostgreSQLMDBDataSource


class PostgreSQLMDBSubselectCoreSourceDefinition(PostgreSQLSubselectCoreSourceDefinition):
    source_cls = PostgreSQLMDBSubselectDataSource


class PostgreSQLMDBCoreConnector(PostgreSQLCoreConnector):
    connection_definitions = (
        PostgreSQLMDBCoreConnectionDefinition,
    )
    source_definitions = (
        PostgreSQLMDBTableCoreSourceDefinition,
        PostgreSQLMDBSubselectCoreSourceDefinition,
    )
    conn_security = frozenset({
        ConnSecuritySettings(MDBConnectionSafetyChecker, frozenset({PostgresConnDTO})),
    })
