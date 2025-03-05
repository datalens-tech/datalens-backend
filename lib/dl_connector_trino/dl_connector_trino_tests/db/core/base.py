import asyncio
from typing import Generator

from frozendict import frozendict
import pytest
import requests
from sqlalchemy.sql.type_api import TypeEngine
from trino.auth import (
    BasicAuthentication,
    JWTAuthentication,
)
from trino.sqlalchemy.datatype import parse_sqltype

from dl_constants.enums import SourceBackendType
from dl_core_testing.database import C
from dl_core_testing.testcases.connection import BaseConnectionTestClass
from dl_db_testing.database.engine_wrapper import DbEngineConfig
from dl_type_transformer.type_transformer import TypeTransformer
from dl_utils.wait import wait_for

from dl_connector_trino.core.adapters import CustomHTTPAdapter
from dl_connector_trino.core.constants import (
    CONNECTION_TYPE_TRINO,
    TrinoAuthType,
)
from dl_connector_trino.core.us_connection import ConnectionTrino
import dl_connector_trino_tests.db.config as test_config


def avoid_get_sa_type(self: C, tt: TypeTransformer, backend_type: SourceBackendType) -> TypeEngine:
    """
    `BaseTrinoTestClass` has `sample_table` fixture that uses `get_sa_type` method from `C` class.
    This method is used to get SQLAlchemy type from user type.
    We need to avoid using this method because it's not implemented for Trino, Trino totally
    relies on `TypeTransformer`.

    This method is used to monkeypatch `get_sa_type` method in `sample_table` fixture.
    """
    native_type = tt.type_user_to_native(user_t=self.user_type)
    return parse_sqltype(native_type.name)


class BaseTrinoTestClass(BaseConnectionTestClass[ConnectionTrino]):
    conn_type = CONNECTION_TYPE_TRINO
    core_test_config = test_config.CORE_TEST_CONFIG

    @pytest.fixture(scope="session", autouse=True)
    def wait_for_trino(self, connection_creation_params: dict) -> None:
        host, port = connection_creation_params["host"], connection_creation_params["port"]
        if connection_creation_params["auth_type"] is TrinoAuthType.NONE:
            scheme = "http"
            auth = None
        else:
            scheme = "https"
            auth = (
                test_config.CorePasswordConnectionSettings.USERNAME,
                test_config.CorePasswordConnectionSettings.PASSWORD,
            )
        post_statement_url = f"{scheme}://{host}:{port}/v1/statement"
        headers = {
            "X-Trino-User": test_config.CorePasswordConnectionSettings.USERNAME,
            "X-Trino-Source": "healthcheck",
        }

        def check_trino_liveness() -> bool:
            try:
                response = requests.post(
                    post_statement_url,
                    headers=headers,
                    auth=auth,
                    data="SELECT 1",
                    verify=False,
                )
                response.raise_for_status()
                if '"QUEUED"' in response.text:
                    return True
                return False
            except requests.RequestException:
                return False

        wait_for(
            name="Trino readiness",
            condition=check_trino_liveness,
            timeout=90,
            require=True,
        )

    @pytest.fixture(scope="class")
    def engine_config(self, db_url: str, engine_params: dict, wait_for_trino: None) -> DbEngineConfig:
        return DbEngineConfig(url=db_url, engine_params=engine_params)

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

    @pytest.fixture(scope="session")
    def connection_creation_params(self) -> dict:
        return dict(
            host=test_config.CoreConnectionSettings.HOST,
            port=test_config.CoreConnectionSettings.PORT,
            username=test_config.CoreConnectionSettings.USERNAME,
            auth_type=TrinoAuthType.NONE,
            **(dict(raw_sql_level=self.raw_sql_level) if self.raw_sql_level is not None else {}),
        )

    @pytest.fixture(scope="class")
    def sample_table_schema(self) -> str:
        monkeypatch = pytest.MonkeyPatch()
        monkeypatch.setattr(C, "get_sa_type", avoid_get_sa_type)
        return "default"


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
        session = requests.Session()
        session.mount("https://", CustomHTTPAdapter(ssl_ca=ssl_ca))
        return session

    @pytest.fixture(scope="session")
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

    @pytest.fixture(scope="session")
    def connection_creation_params(self, ssl_connection_creation_params: dict) -> dict:
        return ssl_connection_creation_params | dict(
            auth_type=TrinoAuthType.PASSWORD,
            password=test_config.CorePasswordConnectionSettings.PASSWORD,
        )


class BaseTrinoJwtTestClass(BaseTrinoSslTestClass):
    @pytest.fixture(scope="session")
    def jwt_session(self, https_session: requests.Session) -> requests.Session:
        auth = JWTAuthentication(test_config.CoreJwtConnectionSettings.JWT)
        session = auth.set_http_session(https_session)
        return session

    @pytest.fixture(scope="class")
    def engine_params(self, jwt_session: requests.Session) -> dict:
        engine_params = {
            "connect_args": frozendict(
                {
                    "http_scheme": "https",
                    "http_session": jwt_session,
                }
            ),
        }
        return engine_params

    @pytest.fixture(scope="session")
    def connection_creation_params(self, ssl_connection_creation_params: dict) -> dict:
        return ssl_connection_creation_params | dict(
            auth_type=TrinoAuthType.JWT,
            jwt=test_config.CoreJwtConnectionSettings.JWT,
        )
