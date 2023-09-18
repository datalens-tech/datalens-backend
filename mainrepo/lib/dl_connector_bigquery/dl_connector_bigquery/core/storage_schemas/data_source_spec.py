import marshmallow.fields as ma_fields

from dl_core.us_manager.storage_schemas.data_source_spec_base import (
    BaseSQLDataSourceSpecStorageSchema,
    SubselectSQLDataSourceSpecStorageSchemaMixin,
    TableSQLDataSourceSpecStorageSchemaMixin,
)

from dl_connector_bigquery.core.data_source_spec import (
    BigQuerySubselectDataSourceSpec,
    BigQueryTableDataSourceSpec,
)


class DatasetSQLDataSourceSpecStorageSchemaMixin(BaseSQLDataSourceSpecStorageSchema):  # noqa
    dataset_name = ma_fields.String(required=True)


class BigQueryTableDataSourceSpecStorageSchema(
    TableSQLDataSourceSpecStorageSchemaMixin,
    DatasetSQLDataSourceSpecStorageSchemaMixin,
    BaseSQLDataSourceSpecStorageSchema,
):
    TARGET_CLS = BigQueryTableDataSourceSpec  # type: ignore


class BigQuerySubselectDataSourceSpecStorageSchema(
    SubselectSQLDataSourceSpecStorageSchemaMixin,
    BaseSQLDataSourceSpecStorageSchema,
):
    TARGET_CLS = BigQuerySubselectDataSourceSpec  # type: ignore
