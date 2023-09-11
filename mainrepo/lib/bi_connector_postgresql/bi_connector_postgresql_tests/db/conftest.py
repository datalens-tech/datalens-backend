from bi_api_lib.loader import ApiLibraryConfig, load_bi_api_lib
from bi_core.loader import CoreLibraryConfig

from bi_core_testing.initialization import initialize_core_test

from bi_formula.loader import load_bi_formula
from bi_formula_testing.forced_literal import forced_literal_use

from bi_connector_postgresql_tests.db.config import BI_TEST_CONFIG, CORE_TEST_CONFIG


pytest_plugins = (
    'aiohttp.pytest_plugin',  # and it, in turn, includes 'pytest_asyncio.plugin'
)


def pytest_configure(config):  # noqa
    load_bi_api_lib(ApiLibraryConfig(
        api_connector_ep_names=BI_TEST_CONFIG.bi_api_connector_whitelist,
        core_lib_config=CoreLibraryConfig(core_connector_ep_names=BI_TEST_CONFIG.core_connector_whitelist),
    ))
    initialize_core_test(pytest_config=config, core_test_config=CORE_TEST_CONFIG)
    load_bi_formula()


__all__ = (
    # auto-use fixtures:
    'forced_literal_use',
)
