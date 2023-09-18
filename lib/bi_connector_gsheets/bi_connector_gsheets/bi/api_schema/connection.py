from __future__ import annotations

from marshmallow import fields as ma_fields

from dl_api_connector.api_schema.connection_base import (
    ConnectionMetaMixin,
    ConnectionSchema,
)
from dl_api_connector.api_schema.connection_base_fields import cache_ttl_field
from dl_api_connector.api_schema.extras import FieldExtra

from bi_connector_gsheets.core.us_connection import GSheetsConnection


class GSheetsConnectionSchema(ConnectionMetaMixin, ConnectionSchema):
    TARGET_CLS = GSheetsConnection

    url = ma_fields.String(attribute="data.url", required=True, bi_extra=FieldExtra(editable=True))
    cache_ttl_sec = cache_ttl_field(attribute="data.cache_ttl_sec")
