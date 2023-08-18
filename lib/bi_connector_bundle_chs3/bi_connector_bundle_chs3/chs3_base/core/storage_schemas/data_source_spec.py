from marshmallow import fields
from marshmallow_enum import EnumField

from bi_constants.enums import FileProcessingStatus

from bi_connector_bundle_chs3.chs3_base.core.data_source_spec import BaseFileS3DataSourceSpec
from bi_core.us_manager.storage_schemas.data_source_spec_base import SQLDataSourceSpecStorageSchema


class BaseFileS3DataSourceSpecStorageSchema(SQLDataSourceSpecStorageSchema):
    TARGET_CLS = BaseFileS3DataSourceSpec

    s3_endpoint = fields.String(required=False, allow_none=True, load_default=None)
    bucket = fields.String(required=False, allow_none=True, load_default=None)
    status = EnumField(FileProcessingStatus, required=False, allow_none=True, load_default=None)
    origin_source_id = fields.String(required=False, allow_none=True, load_default=None)
