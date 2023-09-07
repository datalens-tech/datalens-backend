from bi_core.connectors.sql_base.data_source_migration import DefaultSQLDataSourceMigrator

from bi_connector_clickhouse.core.constants import SOURCE_TYPE_CH_TABLE, SOURCE_TYPE_CH_SUBSELECT


class ClickHouseDataSourceMigrator(DefaultSQLDataSourceMigrator):
    table_source_type = SOURCE_TYPE_CH_TABLE
    subselect_source_type = SOURCE_TYPE_CH_SUBSELECT
    with_db_name = True
