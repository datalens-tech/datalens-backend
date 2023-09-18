from __future__ import annotations

from marshmallow import fields as ma_fields

from dl_api_connector.api_schema.connection_base import (
    ConnectionMetaMixin,
    ConnectionSchema,
)
from dl_api_connector.api_schema.connection_base_fields import cache_ttl_field
from dl_api_connector.api_schema.extras import FieldExtra

from bi_connector_monitoring.core.us_connection import MonitoringConnection


class MonitoringConnectionSchema(ConnectionMetaMixin, ConnectionSchema):
    TARGET_CLS = MonitoringConnection  # type: ignore

    cache_ttl_sec = cache_ttl_field(attribute="data.cache_ttl_sec")
    folder_id = ma_fields.String(
        attribute="data.folder_id",
        required=True,
        allow_none=False,
        bi_extra=FieldExtra(editable=True),
    )
    service_account_id = ma_fields.String(
        attribute="data.service_account_id",
        required=True,
        allow_none=False,
        bi_extra=FieldExtra(editable=True),
    )
