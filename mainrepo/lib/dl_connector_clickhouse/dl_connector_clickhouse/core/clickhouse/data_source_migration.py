from dl_connector_clickhouse.core.clickhouse.constants import (
    SOURCE_TYPE_CH_SUBSELECT,
    SOURCE_TYPE_CH_TABLE,
)
from dl_core.connectors.sql_base.data_source_migration import DefaultSQLDataSourceMigrator


class ClickHouseDataSourceMigrator(DefaultSQLDataSourceMigrator):
    table_source_type = SOURCE_TYPE_CH_TABLE
    subselect_source_type = SOURCE_TYPE_CH_SUBSELECT
    with_db_name = True
