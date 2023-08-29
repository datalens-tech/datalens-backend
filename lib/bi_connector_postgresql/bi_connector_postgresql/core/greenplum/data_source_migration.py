from bi_core.connectors.sql_base.data_source_migration import DefaultSQLDataSourceMigrator

from bi_connector_postgresql.core.greenplum.constants import SOURCE_TYPE_GP_TABLE, SOURCE_TYPE_GP_SUBSELECT


class GreenPlumDataSourceMigrator(DefaultSQLDataSourceMigrator):
    table_source_type = SOURCE_TYPE_GP_TABLE
    subselect_source_type = SOURCE_TYPE_GP_SUBSELECT
    default_schema_name = 'public'
