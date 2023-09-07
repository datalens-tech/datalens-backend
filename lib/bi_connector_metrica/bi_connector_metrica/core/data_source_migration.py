from bi_core.connectors.sql_base.data_source_migration import DefaultSQLDataSourceMigrator

from bi_connector_metrica.core.constants import SOURCE_TYPE_METRICA_API, SOURCE_TYPE_APPMETRICA_API


class MetricaApiDataSourceMigrator(DefaultSQLDataSourceMigrator):
    table_source_type = SOURCE_TYPE_METRICA_API


class AppMetricaApiDataSourceMigrator(DefaultSQLDataSourceMigrator):
    table_source_type = SOURCE_TYPE_APPMETRICA_API
    with_db_name = True
