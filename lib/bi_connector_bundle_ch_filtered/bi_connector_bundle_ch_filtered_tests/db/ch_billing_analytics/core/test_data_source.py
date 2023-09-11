import pytest

from bi_core.data_source_spec.sql import StandardSQLDataSourceSpec

from bi_core_testing.testcases.data_source import SQLDataSourceTestClass

from bi_connector_bundle_ch_filtered.ch_billing_analytics.core.constants import (
    SOURCE_TYPE_CH_BILLING_ANALYTICS_TABLE,
)
from bi_connector_bundle_ch_filtered.ch_billing_analytics.core.us_connection import BillingAnalyticsCHConnection
from bi_connector_bundle_ch_filtered.ch_billing_analytics.core.data_source import BillingAnalyticsCHDataSource

from bi_connector_bundle_ch_filtered_tests.db.ch_billing_analytics.core.base import BaseCHBillingAnalyticsTestClass
from bi_connector_bundle_ch_filtered_tests.db.ch_billing_analytics.config import (
    SR_CONNECTION_SETTINGS, SR_CONNECTION_TABLE_NAME,
)


class TestClickHouseTableDataSource(
        BaseCHBillingAnalyticsTestClass,
        SQLDataSourceTestClass[
            BillingAnalyticsCHConnection,
            StandardSQLDataSourceSpec,
            BillingAnalyticsCHDataSource,
        ],
):

    DSRC_CLS = BillingAnalyticsCHDataSource
    QUERY_PATTERN = 'WHERE billing_account_id IN [\'some_ba_id_1\', \'some_ba_id_2\']'

    @pytest.fixture(scope='class')
    def initial_data_source_spec(self) -> StandardSQLDataSourceSpec:
        dsrc_spec = StandardSQLDataSourceSpec(
            source_type=SOURCE_TYPE_CH_BILLING_ANALYTICS_TABLE,
            db_name=SR_CONNECTION_SETTINGS.DB_NAME,
            table_name=SR_CONNECTION_TABLE_NAME,
        )
        return dsrc_spec
