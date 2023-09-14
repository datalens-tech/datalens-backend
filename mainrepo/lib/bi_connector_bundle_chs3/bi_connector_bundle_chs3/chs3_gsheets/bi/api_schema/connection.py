from __future__ import annotations

from marshmallow import (
    fields,
    validate,
)

from bi_api_connector.api_schema.extras import FieldExtra

from bi_connector_bundle_chs3.chs3_base.bi.api_schema.connection import BaseFileS3ConnectionSchema
from bi_connector_bundle_chs3.chs3_gsheets.bi.api_schema.source import GSheetsFileSourceSchema
from bi_connector_bundle_chs3.chs3_gsheets.core.us_connection import GSheetsFileS3Connection


class GSheetsFileS3ConnectionSchema(BaseFileS3ConnectionSchema):
    TARGET_CLS = GSheetsFileS3Connection

    sources = fields.Nested(
        GSheetsFileSourceSchema,
        many=True,
        attribute="data.sources",
        bi_extra=FieldExtra(editable=True),
        validate=validate.Length(min=1, max=10),
    )

    refresh_token = fields.String(
        attribute="data.refresh_token",
        load_default=None,
        allow_none=True,
        load_only=True,
        bi_extra=FieldExtra(editable=True),
    )
    refresh_enabled = fields.Boolean(attribute="data.refresh_enabled", bi_extra=FieldExtra(editable=True))
    authorized = fields.Boolean(dump_only=True)
