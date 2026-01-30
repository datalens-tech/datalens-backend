from dl_api_lib_testing.initialization import initialize_api_lib_test
from dl_formula_testing.forced_literal import forced_literal_use

from dl_connector_starrocks_tests.db.config import API_TEST_CONFIG


def pytest_configure(config):  # type: ignore  # 2024-01-30 # TODO: fix
    initialize_api_lib_test(pytest_config=config, api_test_config=API_TEST_CONFIG)


__all__ = ("forced_literal_use",)  # auto-use fixture
