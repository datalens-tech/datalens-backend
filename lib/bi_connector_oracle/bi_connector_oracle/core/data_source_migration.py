from dl_core.connectors.sql_base.data_source_migration import DefaultSQLDataSourceMigrator

from bi_connector_oracle.core.constants import SOURCE_TYPE_ORACLE_TABLE, SOURCE_TYPE_ORACLE_SUBSELECT


class OracleDataSourceMigrator(DefaultSQLDataSourceMigrator):
    table_source_type = SOURCE_TYPE_ORACLE_TABLE
    subselect_source_type = SOURCE_TYPE_ORACLE_SUBSELECT
