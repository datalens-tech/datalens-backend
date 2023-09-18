from __future__ import annotations

from dl_api_connector.api_schema.connection_base import (
    ConnectionMetaMixin,
    ConnectionSchema,
)
from dl_api_connector.api_schema.connection_base_fields import cache_ttl_field
from dl_api_connector.api_schema.connection_sql import DBHostField
from dl_api_connector.api_schema.extras import FieldExtra

from bi_connector_solomon.core.us_connection import SolomonConnection


class SolomonConnectionSchema(ConnectionMetaMixin, ConnectionSchema):
    TARGET_CLS = SolomonConnection  # type: ignore

    host = DBHostField(attribute="data.host", required=True, bi_extra=FieldExtra(editable=True))
    cache_ttl_sec = cache_ttl_field(attribute="data.cache_ttl_sec")
