import pytest

from dl_api_lib_testing.initialization import initialize_api_lib_test
from dl_formula_testing.forced_literal import forced_literal_use
from dl_testing.utils import wait_for_initdb

from bi_connector_oracle_tests.db.config import (
    API_TEST_CONFIG,
    INIT_DB_PORT,
)


pytest_plugins = ("aiohttp.pytest_plugin",)  # and it, in turn, includes 'pytest_asyncio.plugin'


def pytest_configure(config):  # noqa
    initialize_api_lib_test(pytest_config=config, api_test_config=API_TEST_CONFIG)


@pytest.fixture(scope="session")
def initdb_ready():
    """Synchronization fixture that ensures that initdb has finished"""
    print("init db wait")
    return wait_for_initdb(initdb_port=INIT_DB_PORT)


__all__ = (
    # auto-use fixtures:
    "forced_literal_use",
)
