from __future__ import annotations

from dl_core.us_connection_base import DataSourceTemplate
from dl_core_testing.testcases.connection import DefaultConnectionTestClass

from bi_connector_usage_tracking_ya_team.core.constants import SOURCE_TYPE_CH_USAGE_TRACKING_YA_TEAM_TABLE
from bi_connector_usage_tracking_ya_team.core.us_connection import UsageTrackingYaTeamConnection
from bi_connector_usage_tracking_ya_team_tests.db.core.base import BaseUsageTrackingYaTeamTestClass
from bi_connector_usage_tracking_ya_team_tests.db.core.config import (
    SR_CONNECTION_SETTINGS,
    SR_CONNECTION_TABLE_NAME,
)


class TestUsageTrackingYaTeamConnection(
    BaseUsageTrackingYaTeamTestClass, DefaultConnectionTestClass[UsageTrackingYaTeamConnection]
):
    def check_data_source_templates(
        self, conn: UsageTrackingYaTeamConnection, dsrc_templates: list[DataSourceTemplate]
    ) -> None:
        expected_template = DataSourceTemplate(
            title=SR_CONNECTION_TABLE_NAME,
            group=[],
            connection_id=conn.uuid,
            source_type=SOURCE_TYPE_CH_USAGE_TRACKING_YA_TEAM_TABLE,
            parameters=dict(db_name=None, table_name=SR_CONNECTION_TABLE_NAME),
        )
        assert [expected_template] == dsrc_templates

    def check_saved_connection(self, conn: UsageTrackingYaTeamConnection, params: dict) -> None:
        assert conn.uuid is not None
        hardcoded_dto_fields = (
            "host",
            "port",
            "username",
            "password",
        )
        conn_dto = conn.get_conn_dto()
        assert conn._us_resp
        us_resp = conn._us_resp.get("data")
        assert us_resp is not None
        for f_name in hardcoded_dto_fields:
            assert us_resp.get(f_name) is None
            assert getattr(conn.data, f_name) is None
            assert getattr(conn_dto, f_name) == getattr(SR_CONNECTION_SETTINGS, f_name.upper())

        hardcoded_properties = (
            "allowed_tables",
            "allow_public_usage",
            "db_name",
        )
        for f_name in hardcoded_properties:
            assert getattr(conn, f_name) is not None
