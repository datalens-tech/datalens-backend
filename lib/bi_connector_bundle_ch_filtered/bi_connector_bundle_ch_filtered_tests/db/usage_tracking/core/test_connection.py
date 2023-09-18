from __future__ import annotations

from bi_connector_bundle_ch_filtered.testing.connection import CHFilteredConnectionTestClass
from bi_connector_bundle_ch_filtered.usage_tracking.core.us_connection import UsageTrackingConnection
from bi_connector_bundle_ch_filtered_tests.db.usage_tracking.config import SR_CONNECTION_SETTINGS
from bi_connector_bundle_ch_filtered_tests.db.usage_tracking.core.base import (
    BaseUsageTrackingTestClass,
    UsageTrackingTestClassWithWrongAuth,
)


class TestUsageTrackingConnection(
    BaseUsageTrackingTestClass,
    CHFilteredConnectionTestClass[UsageTrackingConnection],
):
    sr_connection_settings = SR_CONNECTION_SETTINGS

    def check_saved_connection(self, conn: UsageTrackingConnection, params: dict) -> None:
        assert conn.tenant_id is not None
        super().check_saved_connection(conn, params)


class TestUsageTrackingConnectionWithWrongAuth(UsageTrackingTestClassWithWrongAuth):
    def test_make_connection(self, saved_connection: UsageTrackingConnection) -> None:
        assert saved_connection.tenant_id is None
