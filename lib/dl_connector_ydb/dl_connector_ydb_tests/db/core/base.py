import asyncio
from typing import Generator

import pytest

from dl_core_testing.testcases.connection import BaseConnectionTestClass

from dl_connector_ydb.core.ydb.constants import CONNECTION_TYPE_YDB
from dl_connector_ydb.core.ydb.us_connection import YDBConnection
import dl_connector_ydb_tests.db.config as test_config


class BaseYDBTestClass(BaseConnectionTestClass[YDBConnection]):
    conn_type = CONNECTION_TYPE_YDB
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
            **test_config.CONNECTION_PARAMS,
            **(dict(raw_sql_level=self.raw_sql_level) if self.raw_sql_level is not None else {}),
        )


class BaseSSLYDBTestClass(BaseYDBTestClass):
    @pytest.fixture(scope="class")
    def ssl_ca(self) -> str:
        return test_config.fetch_ca_certificate()

    @pytest.fixture(scope="class")
    def engine_params(self, ssl_ca: str) -> dict:
        return test_config.make_ssl_engine_params(ssl_ca)

    @pytest.fixture(scope="class")
    def db_url(self) -> str:
        return test_config.DB_CORE_URL_SSL

    @pytest.fixture(scope="function")
    def connection_creation_params(self) -> dict:
        return dict(
            **test_config.SSL_CONNECTION_PARAMS,
            **(dict(raw_sql_level=self.raw_sql_level) if self.raw_sql_level is not None else {}),
        )
