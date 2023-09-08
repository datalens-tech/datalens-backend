from bi_core_testing.initialization import initialize_core_test

from bi_formula.loader import load_bi_formula
from bi_formula_testing.forced_literal import forced_literal_use

from bi_connector_clickhouse_tests.db.config import CORE_TEST_CONFIG


pytest_plugins = (
    'aiohttp.pytest_plugin',  # and it, in turn, includes 'pytest_asyncio.plugin'
)


def pytest_configure(config):  # noqa
    initialize_core_test(pytest_config=config, core_test_config=CORE_TEST_CONFIG)
    load_bi_formula()


__all__ = (
    # auto-use fixtures:
    'forced_literal_use',
)
