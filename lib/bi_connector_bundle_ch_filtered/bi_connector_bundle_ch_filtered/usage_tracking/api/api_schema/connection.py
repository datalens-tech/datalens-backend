from __future__ import annotations

from dl_api_connector.api_schema.connection_base import (
    ConnectionMetaMixin,
    ConnectionSchema,
)

from bi_connector_bundle_ch_filtered.usage_tracking.core.us_connection import UsageTrackingConnection


class UsageTrackingConnectionSchema(ConnectionMetaMixin, ConnectionSchema):
    TARGET_CLS = UsageTrackingConnection
