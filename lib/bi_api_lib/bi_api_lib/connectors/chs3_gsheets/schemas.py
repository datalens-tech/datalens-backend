from __future__ import annotations

from marshmallow import fields, validate

from bi_connector_bundle_chs3.chs3_gsheets.core.us_connection import GSheetsFileS3Connection

from bi_api_connector.api_schema.extras import FieldExtra

from bi_api_lib.connectors.chs3_base.schemas import BaseFileSourceSchema, BaseFileS3ConnectionSchema


class GSheetsFileSourceSchema(BaseFileSourceSchema):
    class Meta(BaseFileSourceSchema.Meta):
        target = GSheetsFileS3Connection.FileDataSource

    spreadsheet_id = fields.String(dump_only=True)
    sheet_id = fields.Integer(dump_only=True)
    first_line_is_header = fields.Boolean(dump_only=True)


class GSheetsFileS3ConnectionSchema(BaseFileS3ConnectionSchema):
    TARGET_CLS = GSheetsFileS3Connection

    sources = fields.Nested(
        GSheetsFileSourceSchema,
        many=True,
        attribute='data.sources',
        bi_extra=FieldExtra(editable=True),
        validate=validate.Length(min=1, max=10),
    )

    refresh_token = fields.String(
        attribute='data.refresh_token',
        load_default=None,
        allow_none=True,
        load_only=True,
        bi_extra=FieldExtra(editable=True),
    )
    refresh_enabled = fields.Boolean(attribute='data.refresh_enabled', bi_extra=FieldExtra(editable=True))
    authorized = fields.Boolean(dump_only=True)
