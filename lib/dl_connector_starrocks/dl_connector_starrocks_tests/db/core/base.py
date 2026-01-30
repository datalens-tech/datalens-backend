import asyncio
from typing import Generator

import pytest

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
            db_name=test_config.CoreConnectionSettings.DB_NAME,
            host=test_config.CoreConnectionSettings.HOST,
            port=test_config.CoreConnectionSettings.PORT,
            username=test_config.CoreConnectionSettings.USERNAME,
            password=test_config.CoreConnectionSettings.PASSWORD,
        )
