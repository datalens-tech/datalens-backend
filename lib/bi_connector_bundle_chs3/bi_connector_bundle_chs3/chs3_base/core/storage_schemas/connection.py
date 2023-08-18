from marshmallow import Schema, fields
from marshmallow_enum import EnumField

from bi_constants.enums import FileProcessingStatus

from bi_core.us_manager.storage_schemas.base import DefaultStorageSchema
from bi_core.us_manager.storage_schemas.base_types import SchemaColumnStorageSchema
from bi_core.us_manager.storage_schemas.error_registry import ComponentErrorListSchema


class BaseFileConnectionSourceStorageSchema(DefaultStorageSchema):
    id = fields.String()
    file_id = fields.String(allow_none=True, load_default=None)
    preview_id = fields.String(allow_none=True, load_default=None)
    title = fields.String()
    s3_filename = fields.String(allow_none=True, load_default=None)
    raw_schema = fields.Nested(SchemaColumnStorageSchema, many=True, allow_none=True, load_default=None)
    status = EnumField(FileProcessingStatus)


class BaseFileConnectionDataStorageSchema(Schema):
    sources = fields.Nested(BaseFileConnectionSourceStorageSchema, many=True)
    component_errors = fields.Nested(ComponentErrorListSchema)
