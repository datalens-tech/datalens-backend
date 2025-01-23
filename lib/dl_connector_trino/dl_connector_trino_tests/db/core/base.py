import asyncio
import ssl
from typing import Generator

from frozendict import frozendict
import pytest
import requests
from trino.auth import BasicAuthentication

from dl_core_testing.testcases.connection import BaseConnectionTestClass

from dl_connector_trino.core.constants import (
    CONNECTION_TYPE_TRINO,
    TrinoAuthType,
)
from dl_connector_trino.core.us_connection import ConnectionTrino
import dl_connector_trino_tests.db.config as test_config


class BaseTrinoTestClass(BaseConnectionTestClass[ConnectionTrino]):
    conn_type = CONNECTION_TYPE_TRINO
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
            host=test_config.CoreConnectionSettings.HOST,
            port=test_config.CoreConnectionSettings.PORT,
            username=test_config.CoreConnectionSettings.USERNAME,
            auth_type=TrinoAuthType.NONE,
            **(dict(raw_sql_level=self.raw_sql_level) if self.raw_sql_level is not None else {}),
        )


class BaseTrinoSslTestClass(BaseTrinoTestClass):
    @pytest.fixture(scope="class")
    def db_url(self) -> str:
        return test_config.DB_CORE_SSL_URL

    @pytest.fixture(scope="session")
    def ssl_ca(self) -> str:
        uri = f"{test_config.CoreSslConnectionSettings.CERT_PROVIDER_URL}/ca.pem"
        response = requests.get(uri)
        assert response.status_code == 200, response.text

        return response.text

    @pytest.fixture(scope="session")
    def https_session(self, ssl_ca: str) -> requests.Session:
        class CustomHTTPAdapter(requests.adapters.HTTPAdapter):
            def init_poolmanager(self, *args, **kwargs):
                context = ssl.create_default_context(cadata=ssl_ca)
                context.check_hostname = False
                super().init_poolmanager(*args, **kwargs, ssl_context=context)

        session = requests.Session()
        session.mount("https://", CustomHTTPAdapter())
        return session

    @pytest.fixture(scope="function")
    def ssl_connection_creation_params(self, ssl_ca: str) -> dict:
        return dict(
            host=test_config.CoreSslConnectionSettings.HOST,
            port=test_config.CoreSslConnectionSettings.PORT,
            username=test_config.CoreSslConnectionSettings.USERNAME,
            ssl_ca=ssl_ca,
        )


class BaseTrinoPasswordTestClass(BaseTrinoSslTestClass):
    @pytest.fixture(scope="session")
    def password_session(self, https_session: requests.Session) -> requests.Session:
        auth = BasicAuthentication(
            test_config.CorePasswordConnectionSettings.USERNAME,
            test_config.CorePasswordConnectionSettings.PASSWORD,
        )
        session = auth.set_http_session(https_session)
        return session

    @pytest.fixture(scope="class")
    def engine_params(self, password_session: requests.Session) -> dict:
        engine_params = {
            "connect_args": frozendict(
                {
                    "http_scheme": "https",
                    "http_session": password_session,
                }
            ),
        }
        return engine_params

    @pytest.fixture(scope="function")
    def connection_creation_params(self, ssl_connection_creation_params: dict) -> dict:
        return ssl_connection_creation_params | dict(
            auth_type=TrinoAuthType.PASSWORD,
            password=test_config.CorePasswordConnectionSettings.PASSWORD,
        )
