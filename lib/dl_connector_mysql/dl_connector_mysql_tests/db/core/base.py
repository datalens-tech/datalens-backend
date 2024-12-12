import asyncio
import os
from typing import Generator

from frozendict import frozendict
import pytest
import requests

import dl_configs.utils as bi_configs_utils
from dl_core_testing.testcases.connection import BaseConnectionTestClass

from dl_connector_mysql.core.constants import CONNECTION_TYPE_MYSQL
from dl_connector_mysql.core.us_connection import ConnectionMySQL
import dl_connector_mysql_tests.db.config as test_config


class BaseMySQLTestClass(BaseConnectionTestClass[ConnectionMySQL]):
    conn_type = CONNECTION_TYPE_MYSQL
    core_test_config = test_config.CORE_TEST_CONFIG

    @pytest.fixture(autouse=True)
    # FIXME: This fixture is a temporary solution for failing core tests when they are run together with api tests
    def loop(self, event_loop: asyncio.AbstractEventLoop) -> Generator[asyncio.AbstractEventLoop, None, None]:
        asyncio.set_event_loop(event_loop)
        yield event_loop
        # Attempt to cover an old version of pytest-asyncio:
        # https://github.com/pytest-dev/pytest-asyncio/commit/51d986cec83fdbc14fa08015424c79397afc7ad9
        asyncio.set_event_loop_policy(None)

    @pytest.fixture(scope="class")
    def db_url(self) -> str:
        return test_config.DB_CORE_URL

    @pytest.fixture(scope="function")
    def connection_creation_params(self) -> dict:
        return dict(
            db_name=test_config.CoreConnectionSettings.DB_NAME,
            host=test_config.CoreConnectionSettings.HOST,
            port=test_config.CoreConnectionSettings.PORT,
            username=test_config.CoreConnectionSettings.USERNAME,
            password=test_config.CoreConnectionSettings.PASSWORD,
            **(dict(raw_sql_level=self.raw_sql_level) if self.raw_sql_level is not None else {}),
        )


class BaseSslMySQLTestClass(BaseMySQLTestClass):
    @pytest.fixture(scope="class")
    def ssl_ca(self) -> str:
        uri = f"{test_config.CoreSslConnectionSettings.CERT_PROVIDER_URL}/ca.pem"
        response = requests.get(uri)
        assert response.status_code == 200, response.text

        return response.text

    @pytest.fixture(scope="class")
    def ssl_ca_path(self, ssl_ca: str) -> str:
        ssl_ca_path = os.path.join(bi_configs_utils.get_temp_root_certificates_folder_path(), "ca.pem")
        with open(ssl_ca_path, "w") as f:
            f.write(ssl_ca)

        return ssl_ca_path

    @pytest.fixture(scope="class")
    def engine_params(self, ssl_ca_path: str) -> dict:
        engine_params = {
            "connect_args": frozendict(
                {
                    "ssl": frozendict(
                        {
                            "ca": ssl_ca_path,
                            "check_hostname": False,
                        }
                    ),
                }
            ),
        }
        return engine_params

    @pytest.fixture(scope="class")
    def db_url(self) -> str:
        db_url = test_config.DB_CORE_SSL_URL
        return db_url

    @pytest.fixture(scope="function")
    def connection_creation_params(self, ssl_ca: str) -> dict:
        return dict(
            db_name=test_config.CoreSslConnectionSettings.DB_NAME,
            host=test_config.CoreSslConnectionSettings.HOST,
            port=test_config.CoreSslConnectionSettings.PORT,
            username=test_config.CoreSslConnectionSettings.USERNAME,
            password=test_config.CoreSslConnectionSettings.PASSWORD,
            **(dict(raw_sql_level=self.raw_sql_level) if self.raw_sql_level is not None else {}),
            ssl_enable=True,
            ssl_ca=ssl_ca,
        )
