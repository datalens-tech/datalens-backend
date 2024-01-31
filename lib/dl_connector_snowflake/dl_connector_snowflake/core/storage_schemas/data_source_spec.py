from dl_core.us_manager.storage_schemas.data_source_spec_base import (
    BaseSQLDataSourceSpecStorageSchema,
    SubselectSQLDataSourceSpecStorageSchemaMixin,
    TableSQLDataSourceSpecStorageSchemaMixin,
)

from dl_connector_snowflake.core.data_source_spec import (
    SnowFlakeSubselectDataSourceSpec,
    SnowFlakeTableDataSourceSpec,
)


class SnowFlakeTableDataSourceSpecStorageSchema(
    TableSQLDataSourceSpecStorageSchemaMixin,
    BaseSQLDataSourceSpecStorageSchema,
):
    TARGET_CLS = SnowFlakeTableDataSourceSpec


class SnowFlakeSubselectDataSourceSpecStorageSchema(
    SubselectSQLDataSourceSpecStorageSchemaMixin,
    BaseSQLDataSourceSpecStorageSchema,
):
    TARGET_CLS = SnowFlakeSubselectDataSourceSpec
