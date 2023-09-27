import pytest

from dl_api_lib_testing.configuration import ApiTestEnvironmentConfiguration
from dl_api_lib_testing.connection_base import ConnectionTestBase
from dl_constants.enums import RawSQLLevel
from dl_core_testing.database import (
    CoreDbConfig,
    Db,
)
from dl_core_testing.engine_wrapper import TestingEngineWrapper

from bi_connector_solomon.core.constants import CONNECTION_TYPE_SOLOMON
from bi_connector_solomon_tests.ext.config import API_TEST_CONFIG


class SolomonConnectionTestBase(ConnectionTestBase):
    bi_compeng_pg_on = False
    conn_type = CONNECTION_TYPE_SOLOMON

    @pytest.fixture(scope="class")
    def auth_headers(self, int_cookie: str) -> dict[str, str]:
        return {
            "Cookie": int_cookie,
            "Host": "back.datalens.yandex-team.ru",
            "X-Forwarded-For": "2a02:6b8:0:51e:fd68:81c0:5d34:4531, 2a02:6b8:c12:422b:0:41c8:85ec:0",
        }

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
        return dict(host="solomon.yandex.net")
