import marshmallow.fields as ma_fields

from dl_core.us_manager.storage_schemas.data_source_spec_base import (
    BaseSQLDataSourceSpecStorageSchema,
    SubselectDataSourceSpecStorageSchema,
    TableSQLDataSourceSpecStorageSchemaMixin,
)

from dl_connector_chyt.core.data_source_spec import (
    CHYTSubselectDataSourceSpec,
    CHYTTableDataSourceSpec,
    CHYTTableListDataSourceSpec,
    CHYTTableRangeDataSourceSpec,
)


class CHYTTableDataSourceSpecStorageSchema(TableSQLDataSourceSpecStorageSchemaMixin):
    TARGET_CLS = CHYTTableDataSourceSpec


class CHYTSubselectDataSourceSpecStorageSchema(SubselectDataSourceSpecStorageSchema):
    TARGET_CLS = CHYTSubselectDataSourceSpec


class CHYTTableListDataSourceSpecStorageSchema(BaseSQLDataSourceSpecStorageSchema):
    TARGET_CLS = CHYTTableListDataSourceSpec

    table_names = ma_fields.String(required=False, allow_none=True)


class CHYTTableRangeDataSourceSpecStorageSchema(BaseSQLDataSourceSpecStorageSchema):
    TARGET_CLS = CHYTTableRangeDataSourceSpec

    directory_path = ma_fields.String(required=False, allow_none=True, load_default=None)
    range_from = ma_fields.String(required=False, allow_none=True, load_default=None)
    range_to = ma_fields.String(required=False, allow_none=True, load_default=None)
