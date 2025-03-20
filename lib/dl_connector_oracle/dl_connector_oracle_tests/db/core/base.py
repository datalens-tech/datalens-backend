import asyncio
import ssl
from typing import Generator
import uuid

from frozendict import frozendict
import pytest
import requests

from dl_core_testing.database import Db
from dl_core_testing.testcases.connection import BaseConnectionTestClass

from dl_connector_oracle.core.constants import CONNECTION_TYPE_ORACLE
from dl_connector_oracle.core.us_connection import ConnectionSQLOracle
import dl_connector_oracle_tests.db.config as test_config


class BaseOracleTestClass(BaseConnectionTestClass[ConnectionSQLOracle]):
    conn_type = CONNECTION_TYPE_ORACLE
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
    def connection_creation_params(self, _empty_table) -> dict:
        return dict(
            db_name=test_config.CoreConnectionSettings.DB_NAME,
            host=test_config.CoreConnectionSettings.HOST,
            port=test_config.CoreConnectionSettings.PORT,
            username=test_config.CoreConnectionSettings.USERNAME,
            password=test_config.CoreConnectionSettings.PASSWORD,
            **(dict(raw_sql_level=self.raw_sql_level) if self.raw_sql_level is not None else {}),
        )

    @pytest.fixture(scope="class")
    def _empty_table(self, db: Db) -> None:
        # So that template listings are not empty
        table_name = f"t_{uuid.uuid4().hex[:6]}"
        db.execute(f'CREATE TABLE datalens.{table_name} ("value" VARCHAR2(255))')


class BaseSSLOracleTestClass(BaseOracleTestClass):
    @pytest.fixture(scope="class")
    def ssl_ca(self) -> str:
        uri = f"{test_config.CoreSSLConnectionSettings.CERT_PROVIDER_URL}/ca.pem"
        response = requests.get(uri)
        assert response.status_code == 200, response.text

        return response.text

    @pytest.fixture(scope="class")
    def engine_params(self, ssl_ca: str) -> dict:
        engine_params = {
            "connect_args": frozendict(
                {
                    "ssl_context": ssl.create_default_context(cadata=ssl_ca),
                }
            ),
        }
        return engine_params

    @pytest.fixture(scope="class")
    def db_url(self) -> str:
        return test_config.DB_CORE_URL_SSL

    @pytest.fixture(scope="function")
    def connection_creation_params(self, ssl_ca: str, _empty_table) -> dict:
        return dict(
            db_name=test_config.CoreSSLConnectionSettings.DB_NAME,
            host=test_config.CoreSSLConnectionSettings.HOST,
            port=test_config.CoreSSLConnectionSettings.PORT,
            username=test_config.CoreSSLConnectionSettings.USERNAME,
            password=test_config.CoreSSLConnectionSettings.PASSWORD,
            **(dict(raw_sql_level=self.raw_sql_level) if self.raw_sql_level is not None else {}),
            ssl_enable=True,
            ssl_ca=ssl_ca,
        )
