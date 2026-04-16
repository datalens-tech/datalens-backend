import asyncio
from collections.abc import Generator
import ssl

from frozendict import frozendict
import pytest
import requests

from dl_core.connection_models import TableIdent
from dl_core_testing.database import DbTable
from dl_core_testing.testcases.connection import BaseConnectionTestClass

from dl_connector_starrocks.core.constants import CONNECTION_TYPE_STARROCKS
from dl_connector_starrocks.core.us_connection import ConnectionStarRocks
import dl_connector_starrocks_tests.db.config as test_config


class BaseStarRocksTestClass(BaseConnectionTestClass[ConnectionStarRocks]):
    conn_type = CONNECTION_TYPE_STARROCKS
    core_test_config = test_config.CORE_TEST_CONFIG

    @pytest.fixture(autouse=True)
    def loop(self, event_loop: asyncio.AbstractEventLoop) -> Generator[asyncio.AbstractEventLoop, None, None]:
        asyncio.set_event_loop(event_loop)
        yield event_loop
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
            password=test_config.CoreConnectionSettings.PASSWORD,
            listing_sources=test_config.CoreConnectionSettings.LISTING_SOURCES,
        )

    @pytest.fixture(scope="function")
    def existing_table_ident(self, sample_table: DbTable) -> TableIdent:
        return TableIdent(
            db_name=test_config.CoreConnectionSettings.CATALOG,
            schema_name=sample_table.db.name,
            table_name=sample_table.name,
        )


class BaseSslStarRocksTestClass(BaseStarRocksTestClass):
    @pytest.fixture(scope="class")
    def ssl_ca(self) -> str:
        uri = f"{test_config.CoreSslConnectionSettings.CERT_PROVIDER_URL}/ca.pem"
        response = requests.get(uri)
        assert response.status_code == 200, response.text
        return response.text

    @pytest.fixture(scope="class")
    def engine_params(self, ssl_ca: str) -> dict:
        engine_params = {
            "connect_args": frozendict(
                {
                    "ssl": ssl.create_default_context(cadata=ssl_ca),
                }
            ),
        }
        return engine_params

    @pytest.fixture(scope="class")
    def db_url(self) -> str:
        return test_config.DB_CORE_SSL_URL

    @pytest.fixture(scope="function")
    def connection_creation_params(self, ssl_ca: str) -> dict:
        return dict(
            host=test_config.CoreSslConnectionSettings.HOST,
            port=test_config.CoreSslConnectionSettings.PORT,
            username=test_config.CoreSslConnectionSettings.USERNAME,
            password=test_config.CoreSslConnectionSettings.PASSWORD,
            listing_sources=test_config.CoreSslConnectionSettings.LISTING_SOURCES,
            ssl_enable=True,
            ssl_ca=ssl_ca,
        )
