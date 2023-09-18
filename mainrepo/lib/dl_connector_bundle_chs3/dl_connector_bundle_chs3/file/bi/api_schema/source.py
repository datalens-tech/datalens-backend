from __future__ import annotations

from marshmallow import (
    RAISE,
    Schema,
    fields,
)

from dl_connector_bundle_chs3.chs3_base.bi.api_schema.connection import BaseFileSourceSchema
from dl_connector_bundle_chs3.file.core.us_connection import FileS3Connection
from dl_constants.enums import BIType


class FileSourceColumnTypeSchema(Schema):
    class Meta:
        unknown = RAISE

    name = fields.String()
    user_type = fields.Enum(BIType)


class FileSourceSchema(BaseFileSourceSchema):
    class Meta(BaseFileSourceSchema.Meta):
        target = FileS3Connection.FileDataSource

    column_types = fields.Nested(FileSourceColumnTypeSchema, many=True, load_only=True)
