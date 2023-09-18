import pytest

from dl_api_lib_testing.configuration import BiApiTestEnvironmentConfiguration
from dl_api_lib_testing.connection_base import ConnectionTestBase
from dl_constants.enums import ConnectionType
from dl_core_testing.database import (
    CoreDbConfig,
    Db,
)
from dl_core_testing.engine_wrapper import TestingEngineWrapper

from bi_connector_monitoring.core.constants import CONNECTION_TYPE_MONITORING
from bi_connector_monitoring_tests.db.config import (
    API_CONNECTION_SETTINGS,
    BI_TEST_CONFIG,
)


class MonitoringConnectionTestBase(ConnectionTestBase):
    bi_compeng_pg_on = False
    conn_type = CONNECTION_TYPE_MONITORING

    @pytest.fixture(scope="class")
    def db_url(self) -> str:
        return ""

    @pytest.fixture(scope="class")
    def db(self, db_config: CoreDbConfig) -> Db:
        engine_wrapper = TestingEngineWrapper(config=db_config.engine_config)
        return Db(config=db_config, engine_wrapper=engine_wrapper)

    @pytest.fixture(scope="class")
    def bi_test_config(self) -> BiApiTestEnvironmentConfiguration:
        return BI_TEST_CONFIG

    @pytest.fixture(scope="class")
    def connection_params(self) -> dict:
        return API_CONNECTION_SETTINGS
