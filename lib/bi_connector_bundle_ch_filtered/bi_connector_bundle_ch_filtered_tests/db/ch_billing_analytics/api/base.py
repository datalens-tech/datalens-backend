import pytest

from bi_configs.connectors_settings import ConnectorsSettingsByType

from bi_api_lib_testing.configuration import BiApiTestEnvironmentConfiguration
from bi_api_lib_testing.connection_base import ConnectionTestBase

from bi_connector_clickhouse.db_testing.engine_wrapper import ClickhouseDbEngineConfig
from bi_connector_bundle_ch_filtered_tests.db.config import BI_TEST_CONFIG
from bi_connector_bundle_ch_filtered_tests.db.ch_billing_analytics.core.base import BaseCHBillingAnalyticsTestClass
from bi_connector_bundle_ch_filtered_tests.db.ch_billing_analytics.config import (
    SR_CONNECTION_SETTINGS
)


class CHBillingAnalyticsConnectionTestBase(BaseCHBillingAnalyticsTestClass, ConnectionTestBase):
    bi_compeng_pg_on = False

    @pytest.fixture(scope='class')
    def engine_config(self, db_url: str, engine_params: dict) -> ClickhouseDbEngineConfig:
        return ClickhouseDbEngineConfig(url=db_url, engine_params=engine_params)

    @pytest.fixture(scope='class')
    def bi_test_config(self) -> BiApiTestEnvironmentConfiguration:
        return BI_TEST_CONFIG

    @pytest.fixture(scope='class')
    def connectors_settings(self) -> ConnectorsSettingsByType:
        return ConnectorsSettingsByType(CH_BILLING_ANALYTICS=SR_CONNECTION_SETTINGS)

    @pytest.fixture(scope='class')
    def connection_params(self) -> dict:
        return {}
