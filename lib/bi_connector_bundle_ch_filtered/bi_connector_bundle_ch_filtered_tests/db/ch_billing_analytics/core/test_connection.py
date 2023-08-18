from __future__ import annotations

from bi_connector_bundle_ch_filtered.ch_billing_analytics.core.us_connection import BillingAnalyticsCHConnection

from bi_connector_bundle_ch_filtered.testing.connection import CHFilteredConnectionTestClass
from bi_connector_bundle_ch_filtered_tests.db.ch_billing_analytics.core.base import BaseCHBillingAnalyticsTestClass

from bi_connector_bundle_ch_filtered_tests.db.ch_billing_analytics.config import SR_CONNECTION_SETTINGS


class TestCHBillingAnalyticsConnection(
        BaseCHBillingAnalyticsTestClass,
        CHFilteredConnectionTestClass[BillingAnalyticsCHConnection],
):
    sr_connection_settings = SR_CONNECTION_SETTINGS

    def check_saved_connection(self, conn: BillingAnalyticsCHConnection, params: dict) -> None:
        assert conn.billing_accounts is not None
        assert conn.billing_accounts
        super().check_saved_connection(conn, params)
