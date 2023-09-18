from __future__ import annotations

from marshmallow import fields

from dl_connector_bundle_chs3.chs3_base.bi.api_schema.source import BaseFileSourceSchema
from dl_connector_bundle_chs3.chs3_gsheets.core.us_connection import GSheetsFileS3Connection


class GSheetsFileSourceSchema(BaseFileSourceSchema):
    class Meta(BaseFileSourceSchema.Meta):
        target = GSheetsFileS3Connection.FileDataSource

    spreadsheet_id = fields.String(dump_only=True)
    sheet_id = fields.Integer(dump_only=True)
    first_line_is_header = fields.Boolean(dump_only=True)
