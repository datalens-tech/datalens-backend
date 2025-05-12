from dl_api_lib_testing.initialization import initialize_api_lib_test
from dl_formula_testing.forced_literal import forced_literal_use

from dl_connector_clickhouse_tests.db.config import API_TEST_CONFIG


def pytest_configure(config):  # noqa
    initialize_api_lib_test(pytest_config=config, api_test_config=API_TEST_CONFIG)


__all__ = (
    # auto-use fixtures:
    "forced_literal_use",
)
