from __future__ import annotations

from marshmallow import (
    fields,
    validate,
)

from dl_api_connector.api_schema.extras import FieldExtra

from dl_connector_bundle_chs3.chs3_base.api.api_schema.connection import BaseFileS3ConnectionSchema
from dl_connector_bundle_chs3.chs3_yadocs.api.api_schema.source import YaDocsFileSourceSchema
from dl_connector_bundle_chs3.chs3_yadocs.core.us_connection import YaDocsFileS3Connection


class YaDocsFileS3ConnectionSchema(BaseFileS3ConnectionSchema):
    TARGET_CLS = YaDocsFileS3Connection

    sources = fields.Nested(
        YaDocsFileSourceSchema,
        many=True,
        attribute="data.sources",
        bi_extra=FieldExtra(editable=True),
        validate=validate.Length(min=1, max=10),
    )

    oauth_token = fields.String(
        attribute="data.oauth_token",
        load_default=None,
        allow_none=True,
        load_only=True,
        bi_extra=FieldExtra(editable=True),
    )
    refresh_enabled = fields.Boolean(attribute="data.refresh_enabled", bi_extra=FieldExtra(editable=True))
    authorized = fields.Boolean(dump_only=True)
