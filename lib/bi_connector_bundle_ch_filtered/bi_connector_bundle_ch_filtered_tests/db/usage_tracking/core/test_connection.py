from __future__ import annotations

from dl_core.us_connection_base import DataSourceTemplate

from bi_connector_bundle_ch_filtered.testing.connection import CHFilteredConnectionTestClass
from bi_connector_bundle_ch_filtered.usage_tracking.core.constants import SOURCE_TYPE_CH_USAGE_TRACKING_TABLE
from bi_connector_bundle_ch_filtered.usage_tracking.core.us_connection import UsageTrackingConnection
from bi_connector_bundle_ch_filtered_tests.db.usage_tracking.config import (
    SR_CONNECTION_SETTINGS,
    SR_CONNECTION_TABLE_NAME,
)
from bi_connector_bundle_ch_filtered_tests.db.usage_tracking.core.base import (
    BaseUsageTrackingTestClass,
    UsageTrackingTestClassWithWrongAuth,
)


class TestUsageTrackingConnection(
    BaseUsageTrackingTestClass,
    CHFilteredConnectionTestClass[UsageTrackingConnection],
):
    sr_connection_settings = SR_CONNECTION_SETTINGS

    def check_data_source_templates(
        self, conn: UsageTrackingConnection, dsrc_templates: list[DataSourceTemplate]
    ) -> None:
        expected_template = DataSourceTemplate(
            title=SR_CONNECTION_TABLE_NAME,
            group=[],
            connection_id=conn.uuid,
            source_type=SOURCE_TYPE_CH_USAGE_TRACKING_TABLE,
            parameters=dict(db_name=None, table_name=SR_CONNECTION_TABLE_NAME),
        )
        assert [expected_template] == dsrc_templates

    def check_saved_connection(self, conn: UsageTrackingConnection, params: dict) -> None:
        assert conn.tenant_id is not None
        super().check_saved_connection(conn, params)


class TestUsageTrackingConnectionWithWrongAuth(UsageTrackingTestClassWithWrongAuth):
    def test_make_connection(self, saved_connection: UsageTrackingConnection) -> None:
        assert saved_connection.tenant_id is None
