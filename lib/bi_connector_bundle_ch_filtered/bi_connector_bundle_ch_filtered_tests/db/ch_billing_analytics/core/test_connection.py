from __future__ import annotations

from dl_core.us_connection_base import DataSourceTemplate

from bi_connector_bundle_ch_filtered.ch_billing_analytics.core.constants import SOURCE_TYPE_CH_BILLING_ANALYTICS_TABLE
from bi_connector_bundle_ch_filtered.ch_billing_analytics.core.us_connection import BillingAnalyticsCHConnection
from bi_connector_bundle_ch_filtered.testing.connection import CHFilteredConnectionTestClass
from bi_connector_bundle_ch_filtered_tests.db.ch_billing_analytics.config import (
    SR_CONNECTION_DB_NAME,
    SR_CONNECTION_SETTINGS,
    SR_CONNECTION_TABLE_NAME,
)
from bi_connector_bundle_ch_filtered_tests.db.ch_billing_analytics.core.base import BaseCHBillingAnalyticsTestClass


class TestCHBillingAnalyticsConnection(
    BaseCHBillingAnalyticsTestClass,
    CHFilteredConnectionTestClass[BillingAnalyticsCHConnection],
):
    sr_connection_settings = SR_CONNECTION_SETTINGS

    def check_data_source_templates(
        self, conn: BillingAnalyticsCHConnection, dsrc_templates: list[DataSourceTemplate]
    ) -> None:
        expected_template = DataSourceTemplate(
            title=SR_CONNECTION_TABLE_NAME,
            group=[SR_CONNECTION_DB_NAME],
            connection_id=conn.uuid,
            source_type=SOURCE_TYPE_CH_BILLING_ANALYTICS_TABLE,
            parameters=dict(db_name=SR_CONNECTION_DB_NAME, table_name=SR_CONNECTION_TABLE_NAME),
        )
        assert [expected_template] == dsrc_templates

    def check_saved_connection(self, conn: BillingAnalyticsCHConnection, params: dict) -> None:
        assert conn.billing_accounts is not None
        assert conn.billing_accounts
        super().check_saved_connection(conn, params)
