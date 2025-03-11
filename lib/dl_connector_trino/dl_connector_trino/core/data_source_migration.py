from dl_core.connectors.sql_base.data_source_migration import DefaultSQLDataSourceMigrator
from dl_core.data_source_spec.sql import StandardSchemaSQLDataSourceSpec

from dl_connector_trino.core.constants import (
    SOURCE_TYPE_TRINO_SUBSELECT,
    SOURCE_TYPE_TRINO_TABLE,
)


class TrinoDataSourceMigrator(DefaultSQLDataSourceMigrator):
    table_source_type = SOURCE_TYPE_TRINO_TABLE
    table_dsrc_spec_cls = StandardSchemaSQLDataSourceSpec
    subselect_source_type = SOURCE_TYPE_TRINO_SUBSELECT
    with_db_name = True
