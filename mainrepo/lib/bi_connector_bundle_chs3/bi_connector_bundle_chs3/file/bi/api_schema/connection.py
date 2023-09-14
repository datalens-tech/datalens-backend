from __future__ import annotations

from marshmallow import (
    fields,
    validate,
)

from bi_api_connector.api_schema.extras import FieldExtra

from bi_connector_bundle_chs3.chs3_base.bi.api_schema.connection import BaseFileS3ConnectionSchema
from bi_connector_bundle_chs3.file.bi.api_schema.source import FileSourceSchema
from bi_connector_bundle_chs3.file.core.us_connection import FileS3Connection


class FileS3ConnectionSchema(BaseFileS3ConnectionSchema):
    TARGET_CLS = FileS3Connection

    sources = fields.Nested(
        FileSourceSchema,
        many=True,
        attribute="data.sources",
        bi_extra=FieldExtra(editable=True),
        validate=validate.Length(min=1, max=10),
    )
