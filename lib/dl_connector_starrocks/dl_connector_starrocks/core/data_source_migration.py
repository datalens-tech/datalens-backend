from dl_core.connectors.sql_base.data_source_migration import DefaultSQLDataSourceMigrator

from dl_connector_starrocks.core.constants import (
    SOURCE_TYPE_STARROCKS_SUBSELECT,
    SOURCE_TYPE_STARROCKS_TABLE,
)


class StarRocksDataSourceMigrator(DefaultSQLDataSourceMigrator):
    table_source_type = SOURCE_TYPE_STARROCKS_TABLE
    subselect_source_type = SOURCE_TYPE_STARROCKS_SUBSELECT
