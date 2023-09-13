from bi_connector_postgresql.core.postgresql.connector import (
    PostgreSQLCoreConnectionDefinition,
    PostgreSQLCoreConnector,
    PostgreSQLTableCoreSourceDefinition,
    PostgreSQLSubselectCoreSourceDefinition,
)
from bi_connector_postgresql_mdb.core.data_source import PostgreSQLMDBDataSource, PostgreSQLMDBSubselectDataSource
from bi_connector_postgresql_mdb.core.us_connection import ConnectionPostgreSQLMDB
from bi_connector_postgresql_mdb.core.storage_schemas import ConnectionPostgreSQLMDBDataStorageSchema
from bi_connector_postgresql_mdb.core.settings import PostgreSQLMDBSettingDefinition
from bi_connector_postgresql.core.postgresql.dto import PostgresConnDTO


class PostgreSQLMDBCoreConnectionDefinition(PostgreSQLCoreConnectionDefinition):
    connection_cls = ConnectionPostgreSQLMDB
    us_storage_schema_cls = ConnectionPostgreSQLMDBDataStorageSchema
    settings_definition = PostgreSQLMDBSettingDefinition


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
    mdb_dto_classes = frozenset({PostgresConnDTO})
