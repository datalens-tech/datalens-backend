import marshmallow.fields as ma_fields

from bi_core.us_manager.storage_schemas.data_source_spec_base import (
    SQLDataSourceSpecStorageSchema, SubselectDataSourceSpecStorageSchema
)
from bi_core.connectors.chydb.data_source_spec import (
    CHYDBTableDataSourceSpec, CHYDBSubselectDataSourceSpec,
)


class CHYDBTableDataSourceSpecStorageSchema(SQLDataSourceSpecStorageSchema):
    TARGET_CLS = CHYDBTableDataSourceSpec

    ydb_cluster = ma_fields.String(required=False, allow_none=True, load_default=None)
    ydb_database = ma_fields.String(required=False, allow_none=True, load_default=None)


class CHYDBSubselectDataSourceSpecStorageSchema(SubselectDataSourceSpecStorageSchema):
    TARGET_CLS = CHYDBSubselectDataSourceSpec
