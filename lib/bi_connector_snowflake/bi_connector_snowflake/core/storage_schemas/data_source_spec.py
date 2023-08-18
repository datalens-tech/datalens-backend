from bi_connector_snowflake.core.data_source_spec import (
    SnowFlakeTableDataSourceSpec,
    SnowFlakeSubselectDataSourceSpec,
)

from bi_core.us_manager.storage_schemas.data_source_spec_base import (
    BaseSQLDataSourceSpecStorageSchema,
    TableSQLDataSourceSpecStorageSchemaMixin,
    SubselectSQLDataSourceSpecStorageSchemaMixin,
)


class SnowFlakeTableDataSourceSpecStorageSchema(
        TableSQLDataSourceSpecStorageSchemaMixin,
        BaseSQLDataSourceSpecStorageSchema,
):
    TARGET_CLS = SnowFlakeTableDataSourceSpec  # type: ignore


class SnowFlakeSubselectDataSourceSpecStorageSchema(
        SubselectSQLDataSourceSpecStorageSchemaMixin,
        BaseSQLDataSourceSpecStorageSchema,
):
    TARGET_CLS = SnowFlakeSubselectDataSourceSpec  # type: ignore
