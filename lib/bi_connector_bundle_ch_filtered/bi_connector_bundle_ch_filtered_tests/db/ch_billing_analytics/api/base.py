import pytest

from bi_api_lib_testing_ya.configuration import ApiTestEnvironmentConfigurationPrivate
from bi_api_lib_testing_ya.connection_base import ConnectionTestPrivateBase
from dl_configs.connectors_settings import ConnectorSettingsBase
from dl_connector_clickhouse.db_testing.engine_wrapper import ClickhouseDbEngineConfig
from dl_constants.enums import ConnectionType

from bi_connector_bundle_ch_filtered_tests.db.ch_billing_analytics.config import SR_CONNECTION_SETTINGS
from bi_connector_bundle_ch_filtered_tests.db.ch_billing_analytics.core.base import BaseCHBillingAnalyticsTestClass
from bi_connector_bundle_ch_filtered_tests.db.config import API_TEST_CONFIG


class CHBillingAnalyticsConnectionTestBase(BaseCHBillingAnalyticsTestClass, ConnectionTestPrivateBase):
    bi_compeng_pg_on = False

    @pytest.fixture(scope="class")
    def engine_config(self, db_url: str, engine_params: dict) -> ClickhouseDbEngineConfig:
        return ClickhouseDbEngineConfig(url=db_url, engine_params=engine_params)

    @pytest.fixture(scope="class")
    def bi_test_config(self) -> ApiTestEnvironmentConfigurationPrivate:
        return API_TEST_CONFIG

    @pytest.fixture(scope="class")
    def connectors_settings(self) -> dict[ConnectionType, ConnectorSettingsBase]:
        return {self.conn_type: SR_CONNECTION_SETTINGS}

    @pytest.fixture(scope="class")
    def connection_params(self) -> dict:
        return {}
