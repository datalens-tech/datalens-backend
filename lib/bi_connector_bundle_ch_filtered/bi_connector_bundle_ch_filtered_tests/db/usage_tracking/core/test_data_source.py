import pytest

from bi_core.data_source_spec.sql import StandardSQLDataSourceSpec

from bi_core_testing.testcases.data_source import SQLDataSourceTestClass

from bi_connector_bundle_ch_filtered.usage_tracking.core.constants import (
    SOURCE_TYPE_CH_USAGE_TRACKING_TABLE,
)
from bi_connector_bundle_ch_filtered.usage_tracking.core.us_connection import UsageTrackingConnection
from bi_connector_bundle_ch_filtered.usage_tracking.core.data_source import UsageTrackingDataSource

from bi_connector_bundle_ch_filtered_tests.db.usage_tracking.core.base import BaseUsageTrackingTestClass

from bi_connector_bundle_ch_filtered_tests.db.usage_tracking.config import (
    SR_CONNECTION_SETTINGS, SR_CONNECTION_TABLE_NAME,
)


class TestUsageTrackingDataSource(
        BaseUsageTrackingTestClass,
        SQLDataSourceTestClass[
            UsageTrackingConnection,
            StandardSQLDataSourceSpec,
            UsageTrackingDataSource,
        ],
):

    DSRC_CLS = UsageTrackingDataSource
    QUERY_PATTERN = 'WHERE folder_id = \'folder_1\''

    @pytest.fixture(scope='class')
    def initial_data_source_spec(self) -> StandardSQLDataSourceSpec:
        dsrc_spec = StandardSQLDataSourceSpec(
            source_type=SOURCE_TYPE_CH_USAGE_TRACKING_TABLE,
            db_name=SR_CONNECTION_SETTINGS.DB_NAME,
            table_name=SR_CONNECTION_TABLE_NAME,
        )
        return dsrc_spec
