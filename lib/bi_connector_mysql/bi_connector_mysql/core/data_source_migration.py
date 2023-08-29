from bi_core.connectors.sql_base.data_source_migration import DefaultSQLDataSourceMigrator

from bi_connector_mysql.core.constants import SOURCE_TYPE_MYSQL_TABLE, SOURCE_TYPE_MYSQL_SUBSELECT


class MySQLDataSourceMigrator(DefaultSQLDataSourceMigrator):
    table_source_type = SOURCE_TYPE_MYSQL_TABLE
    subselect_source_type = SOURCE_TYPE_MYSQL_SUBSELECT
