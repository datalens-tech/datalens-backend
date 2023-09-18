import pytest

from dl_core_testing.initialization import initialize_core_test
from dl_formula.loader import load_bi_formula
from dl_formula_testing.forced_literal import forced_literal_use
from dl_testing.utils import wait_for_initdb

from bi_connector_oracle_tests.db.config import (
    CORE_TEST_CONFIG,
    INIT_DB_PORT,
)

pytest_plugins = ("aiohttp.pytest_plugin",)  # and it, in turn, includes 'pytest_asyncio.plugin'


def pytest_configure(config):  # noqa
    print("initialize core tests")
    initialize_core_test(pytest_config=config, core_test_config=CORE_TEST_CONFIG)
    load_bi_formula()


@pytest.fixture(scope="session")
def initdb_ready():
    """Synchronization fixture that ensures that initdb has finished"""
    print("init db wait")
    return wait_for_initdb(initdb_port=INIT_DB_PORT)


__all__ = (
    # auto-use fixtures:
    "forced_literal_use",
)
