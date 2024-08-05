import pytest

from dl_api_lib_testing.configuration import ApiTestEnvironmentConfiguration
from dl_api_lib_testing.connection_base import ConnectionTestBase
from dl_constants.enums import RawSQLLevel
from dl_core_testing.database import (
    CoreDbConfig,
    Db,
)
from dl_core_testing.engine_wrapper import TestingEngineWrapper

from dl_connector_promql.core.constants import CONNECTION_TYPE_PROMQL
from dl_connector_promql_tests.db.config import (
    API_CONNECTION_SETTINGS,
    API_TEST_CONFIG,
)


class PromQLConnectionTestBase(ConnectionTestBase):
    compeng_enabled = False
    conn_type = CONNECTION_TYPE_PROMQL

    @pytest.fixture(scope="class")
    def db_url(self) -> str:
        return ""

    @pytest.fixture(scope="class")
    def db(self, db_config: CoreDbConfig) -> Db:
        engine_wrapper = TestingEngineWrapper(config=db_config.engine_config)
        return Db(config=db_config, engine_wrapper=engine_wrapper)

    @pytest.fixture(scope="class")
    def bi_test_config(self) -> ApiTestEnvironmentConfiguration:
        return API_TEST_CONFIG

    @pytest.fixture(scope="class")
    def connection_params(self) -> dict:
        return API_CONNECTION_SETTINGS


class PromQLDashSQLConnectionTest(PromQLConnectionTestBase):
    @pytest.fixture(scope="class")
    def connection_params(self) -> dict:
        return API_CONNECTION_SETTINGS | dict(
            raw_sql_level=RawSQLLevel.dashsql.value,
        )
