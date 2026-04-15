import pytest

from dl_api_lib_testing.initialization import initialize_api_lib_test

from dl_connector_bitrix_gds_tests.ext.config import API_TEST_CONFIG
from dl_connector_bitrix_gds_tests.ext.settings import Settings


def pytest_configure(config):  # noqa
    initialize_api_lib_test(pytest_config=config, api_test_config=API_TEST_CONFIG)


@pytest.fixture(scope="session")
def settings() -> Settings:
    return Settings()


@pytest.fixture(scope="session")
def bitrix_datalens_token(settings: Settings):
    return settings.BITRIX_DATALENS_TOKEN
