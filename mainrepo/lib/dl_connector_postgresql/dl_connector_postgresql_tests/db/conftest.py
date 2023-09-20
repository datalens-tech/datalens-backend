from dl_api_lib_testing.initialization import initialize_api_lib_test
from dl_connector_postgresql_tests.db.config import BI_TEST_CONFIG
from dl_formula_testing.forced_literal import forced_literal_use


pytest_plugins = ("aiohttp.pytest_plugin",)  # and it, in turn, includes 'pytest_asyncio.plugin'


def pytest_configure(config):  # noqa
    initialize_api_lib_test(pytest_config=config, api_test_config=BI_TEST_CONFIG)


__all__ = (
    # auto-use fixtures:
    "forced_literal_use",
)
