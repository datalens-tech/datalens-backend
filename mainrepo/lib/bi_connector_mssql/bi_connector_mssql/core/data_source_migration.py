from bi_core.data_source_spec.sql import StandardSchemaSQLDataSourceSpec
from bi_core.connectors.sql_base.data_source_migration import DefaultSQLDataSourceMigrator

from bi_connector_mssql.core.constants import SOURCE_TYPE_MSSQL_TABLE, SOURCE_TYPE_MSSQL_SUBSELECT


class MSSQLDataSourceMigrator(DefaultSQLDataSourceMigrator):
    table_source_type = SOURCE_TYPE_MSSQL_TABLE
    table_dsrc_spec_cls = StandardSchemaSQLDataSourceSpec
    subselect_source_type = SOURCE_TYPE_MSSQL_SUBSELECT
