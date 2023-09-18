from dl_connector_postgresql.core.postgresql.constants import (
    SOURCE_TYPE_PG_SUBSELECT,
    SOURCE_TYPE_PG_TABLE,
)
from dl_core.connectors.sql_base.data_source_migration import DefaultSQLDataSourceMigrator


class PostgreSQLDataSourceMigrator(DefaultSQLDataSourceMigrator):
    table_source_type = SOURCE_TYPE_PG_TABLE
    subselect_source_type = SOURCE_TYPE_PG_SUBSELECT
    default_schema_name = "public"
