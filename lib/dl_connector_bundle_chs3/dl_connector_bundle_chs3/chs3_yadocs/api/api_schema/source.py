from __future__ import annotations

from marshmallow import fields

from dl_connector_bundle_chs3.chs3_base.api.api_schema.source import BaseFileSourceSchema
from dl_connector_bundle_chs3.chs3_yadocs.core.us_connection import YaDocsFileS3Connection


class YaDocsFileSourceSchema(BaseFileSourceSchema):
    class Meta(BaseFileSourceSchema.Meta):
        target = YaDocsFileS3Connection.FileDataSource

    public_link = fields.String(dump_only=True)
    private_path = fields.String(dump_only=True)
    sheet_id = fields.String(dump_only=True)
    first_line_is_header = fields.Boolean(dump_only=True)
