import pytest

from dl_core.data_source_spec.sql import StandardSQLDataSourceSpec
from dl_core_testing.testcases.data_source import SQLDataSourceTestClass

from bi_connector_bundle_ch_filtered.usage_tracking.core.constants import SOURCE_TYPE_CH_USAGE_TRACKING_TABLE
from bi_connector_usage_tracking_ya_team.core.data_source import UsageTrackingYaTeamDataSource
from bi_connector_usage_tracking_ya_team.core.us_connection import UsageTrackingYaTeamConnection
from bi_connector_usage_tracking_ya_team_tests.db.core.base import BaseUsageTrackingYaTeamTestClass
from bi_connector_usage_tracking_ya_team_tests.db.core.config import (
    SR_CONNECTION_SETTINGS,
    SR_CONNECTION_TABLE_NAME,
)


class TestUsageTrackingYaTeamDataSource(
    BaseUsageTrackingYaTeamTestClass,
    SQLDataSourceTestClass[
        UsageTrackingYaTeamConnection,
        StandardSQLDataSourceSpec,
        UsageTrackingYaTeamDataSource,
    ],
):
    DSRC_CLS = UsageTrackingYaTeamDataSource
    QUERY_PATTERN = "WHERE user_id = 'datalens_user_id'"

    @pytest.fixture(scope="class")
    def initial_data_source_spec(self) -> StandardSQLDataSourceSpec:
        dsrc_spec = StandardSQLDataSourceSpec(
            source_type=SOURCE_TYPE_CH_USAGE_TRACKING_TABLE,
            db_name=SR_CONNECTION_SETTINGS.DB_NAME,
            table_name=SR_CONNECTION_TABLE_NAME,
        )
        return dsrc_spec
