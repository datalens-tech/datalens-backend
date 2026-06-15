import asyncio
from collections.abc import Generator

import pytest

from dl_core_testing.testcases.connection import BaseConnectionTestClass

from dl_connector_greenplum.core.constants import CONNECTION_TYPE_GREENPLUM
from dl_connector_greenplum.core.us_connection import GreenplumConnection
import dl_connector_greenplum_tests.db.config as test_config


class BaseGreenplumTestClass(BaseConnectionTestClass[GreenplumConnection]):
    conn_type = CONNECTION_TYPE_GREENPLUM
    core_test_config = test_config.CORE_TEST_CONFIG

    @pytest.fixture(autouse=True)
    def loop(self, event_loop: asyncio.AbstractEventLoop) -> Generator[asyncio.AbstractEventLoop, None, None]:
        asyncio.set_event_loop(event_loop)
        yield event_loop
        asyncio.set_event_loop_policy(None)

    @pytest.fixture(scope="class")
    def db_url(self) -> str:
        return test_config.DB_URLS[test_config.GreenplumVersion.GP6]

    @pytest.fixture
    def connection_creation_params(self) -> dict:
        return dict(
            **test_config.CONNECTION_PARAMS_BY_VERSION[test_config.GreenplumVersion.GP6],
            **({"raw_sql_level": self.raw_sql_level} if self.raw_sql_level is not None else {}),
        )
