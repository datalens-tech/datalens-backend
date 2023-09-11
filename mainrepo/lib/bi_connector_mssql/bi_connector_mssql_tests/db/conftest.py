import pytest

from bi_api_lib.loader import ApiLibraryConfig, load_bi_api_lib

from bi_core_testing.initialization import initialize_core_test

from bi_formula.loader import load_bi_formula
from bi_formula_testing.forced_literal import forced_literal_use

from bi_testing.utils import wait_for_initdb
from bi_connector_mssql_tests.db.config import BI_TEST_CONFIG, CORE_TEST_CONFIG, INIT_DB_PORT


pytest_plugins = (
    'aiohttp.pytest_plugin',  # and it, in turn, includes 'pytest_asyncio.plugin'
)


def pytest_configure(config):  # noqa
    load_bi_api_lib(ApiLibraryConfig(api_connector_ep_names=BI_TEST_CONFIG.bi_api_connector_whitelist))
    initialize_core_test(pytest_config=config, core_test_config=CORE_TEST_CONFIG)
    load_bi_formula()


@pytest.fixture(scope='session')
def initdb_ready():
    """ Synchronization fixture that ensures that initdb has finished """
    return wait_for_initdb(initdb_port=INIT_DB_PORT)


__all__ = (
    # auto-use fixtures:
    'forced_literal_use',
)
