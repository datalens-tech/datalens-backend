from dl_core.connectors.sql_base.data_source_migration import DefaultSQLDataSourceMigrator

from dl_connector_greenplum.core.constants import (
    SOURCE_TYPE_GP_SUBSELECT,
    SOURCE_TYPE_GP_TABLE,
)
from dl_connector_postgresql.core.postgresql.data_source_migration import PostgreSQLDataSourceMigrator


class GreenPlumDataSourceMigrator(PostgreSQLDataSourceMigrator):
    table_source_type = SOURCE_TYPE_GP_TABLE
    subselect_source_type = SOURCE_TYPE_GP_SUBSELECT
