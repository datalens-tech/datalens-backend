import pytest

from dl_core.data_source_spec.sql import StandardSQLDataSourceSpec

from bi_connector_bundle_ch_filtered_ya_cloud.market_couriers.core.constants import (
    SOURCE_TYPE_CH_MARKET_COURIERS_TABLE,
)
from bi_connector_bundle_ch_filtered_ya_cloud.market_couriers.core.us_connection import (
    ConnectionClickhouseMarketCouriers,
)
from bi_connector_bundle_ch_filtered_ya_cloud.market_couriers.core.data_source import (
    ClickHouseMarketCouriersDataSource
)
from dl_core_testing.testcases.data_source import SQLDataSourceTestClass

from bi_connector_bundle_ch_filtered_ya_cloud_tests.ext.market_couriers.core.base import BaseMarketCouriersTestClass

from bi_connector_bundle_ch_filtered_ya_cloud_tests.ext.config import EXT_BLACKBOX_USER_UID
from bi_connector_bundle_ch_filtered_ya_cloud_tests.ext.config import (
    SR_CONNECTION_SETTINGS_PARAMS, SR_CONNECTION_TABLE_NAME,
)


class TestClickHouseTableDataSource(
        BaseMarketCouriersTestClass,
        SQLDataSourceTestClass[
            ConnectionClickhouseMarketCouriers,
            StandardSQLDataSourceSpec,
            ClickHouseMarketCouriersDataSource,
        ],
):

    DSRC_CLS = ClickHouseMarketCouriersDataSource
    QUERY_PATTERN = f'WHERE int_value = {EXT_BLACKBOX_USER_UID}'

    @pytest.fixture(scope='class')
    def initial_data_source_spec(self) -> StandardSQLDataSourceSpec:
        dsrc_spec = StandardSQLDataSourceSpec(
            source_type=SOURCE_TYPE_CH_MARKET_COURIERS_TABLE,
            db_name=SR_CONNECTION_SETTINGS_PARAMS['DB_NAME'],
            table_name=SR_CONNECTION_TABLE_NAME,
        )
        return dsrc_spec
