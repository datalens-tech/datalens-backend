from __future__ import annotations

from marshmallow import fields, validate

from bi_connector_bundle_chs3.file.core.us_connection import FileS3Connection

from bi_api_connector.api_schema.extras import FieldExtra

from bi_api_lib.connectors.chs3_base.schemas import BaseFileSourceSchema, BaseFileS3ConnectionSchema, FileSourceColumnTypeSchema


class FileSourceSchema(BaseFileSourceSchema):
    class Meta(BaseFileSourceSchema.Meta):
        target = FileS3Connection.FileDataSource

    column_types = fields.Nested(FileSourceColumnTypeSchema, many=True, load_only=True)


class FileS3ConnectionSchema(BaseFileS3ConnectionSchema):
    TARGET_CLS = FileS3Connection

    sources = fields.Nested(
        FileSourceSchema,
        many=True,
        attribute='data.sources',
        bi_extra=FieldExtra(editable=True),
        validate=validate.Length(min=1, max=10),
    )
