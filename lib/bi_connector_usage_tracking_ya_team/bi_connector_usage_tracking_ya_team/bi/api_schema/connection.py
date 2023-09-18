from __future__ import annotations

from bi_connector_usage_tracking_ya_team.core.us_connection import UsageTrackingYaTeamConnection

from dl_api_connector.api_schema.connection_base import ConnectionSchema, ConnectionMetaMixin


class UsageTrackingYaTeamConnectionSchema(ConnectionMetaMixin, ConnectionSchema):
    TARGET_CLS = UsageTrackingYaTeamConnection
