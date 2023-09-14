import pytest

from bi_api_lib_testing.configuration import BiApiTestEnvironmentConfiguration
from bi_api_lib_testing.connection_base import ConnectionTestBase
from bi_constants.enums import (
    ConnectionType,
    RawSQLLevel,
)
from bi_core_testing.database import (
    CoreDbConfig,
    Db,
)
from bi_core_testing.engine_wrapper import TestingEngineWrapper

from bi_connector_promql.core.constants import CONNECTION_TYPE_PROMQL
from bi_connector_promql_tests.db.config import (
    API_CONNECTION_SETTINGS,
    BI_TEST_CONFIG,
)


class PromQLConnectionTestBase(ConnectionTestBase):
    bi_compeng_pg_on = False
    conn_type = CONNECTION_TYPE_PROMQL

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


class PromQLDashSQLConnectionTest(PromQLConnectionTestBase):
    @pytest.fixture(scope="class")
    def connection_params(self) -> dict:
        return API_CONNECTION_SETTINGS | dict(
            raw_sql_level=RawSQLLevel.dashsql.value,
        )
