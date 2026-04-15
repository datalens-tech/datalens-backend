import pytest

from dl_api_lib_testing.initialization import initialize_api_lib_test
from dl_formula_testing.forced_literal import forced_literal_use

from dl_connector_bigquery_tests.ext.config import API_TEST_CONFIG
from dl_connector_bigquery_tests.ext.settings import Settings


def pytest_configure(config):  # noqa
    initialize_api_lib_test(pytest_config=config, api_test_config=API_TEST_CONFIG)


@pytest.fixture(scope="session")
def settings() -> Settings:
    return Settings()


__all__ = (
    # auto-use fixtures:
    "forced_literal_use",
)
