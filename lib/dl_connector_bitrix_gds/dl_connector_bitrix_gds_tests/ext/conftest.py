import os

import pytest

from dl_api_lib_testing.initialization import initialize_api_lib_test
from dl_testing.env_params.generic import GenericEnvParamGetter

from dl_connector_bitrix_gds_tests.ext.config import API_TEST_CONFIG


def pytest_configure(config):  # noqa
    initialize_api_lib_test(pytest_config=config, api_test_config=API_TEST_CONFIG)


@pytest.fixture(scope="session")
def env_param_getter() -> GenericEnvParamGetter:
    filepath = os.path.join(os.path.dirname(__file__), "params.yml")
    return GenericEnvParamGetter.from_yaml_file(filepath)


@pytest.fixture(scope="session")
def bitrix_datalens_token(env_param_getter):
    return env_param_getter.get_str_value("BITRIX_DATALENS_TOKEN")
