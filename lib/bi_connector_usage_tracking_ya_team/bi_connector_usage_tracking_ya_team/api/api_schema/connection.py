from __future__ import annotations

from dl_api_connector.api_schema.connection_base import (
    ConnectionMetaMixin,
    ConnectionSchema,
)

from bi_connector_usage_tracking_ya_team.core.us_connection import UsageTrackingYaTeamConnection


class UsageTrackingYaTeamConnectionSchema(ConnectionMetaMixin, ConnectionSchema):
    TARGET_CLS = UsageTrackingYaTeamConnection
