from dl_core.connectors.sql_base.data_source_migration import DefaultSQLDataSourceMigrator
from dl_core.data_source_spec.sql import StandardSchemaSQLDataSourceSpec

from dl_connector_postgresql.core.postgresql.constants import (
    SOURCE_TYPE_PG_SUBSELECT,
    SOURCE_TYPE_PG_TABLE,
)


class PostgreSQLDataSourceMigrator(DefaultSQLDataSourceMigrator):
    table_source_type = SOURCE_TYPE_PG_TABLE
    table_dsrc_spec_cls = StandardSchemaSQLDataSourceSpec
    subselect_source_type = SOURCE_TYPE_PG_SUBSELECT

    default_schema_name = "public"
