from dl_connector_greenplum.core.constants import (
    SOURCE_TYPE_GP_SUBSELECT,
    SOURCE_TYPE_GP_TABLE,
)
from dl_core.connectors.sql_base.data_source_migration import DefaultSQLDataSourceMigrator


class GreenPlumDataSourceMigrator(DefaultSQLDataSourceMigrator):
    table_source_type = SOURCE_TYPE_GP_TABLE
    subselect_source_type = SOURCE_TYPE_GP_SUBSELECT
    default_schema_name = "public"
