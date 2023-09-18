import os

import pytest

from dl_core_testing.initialization import initialize_core_test
from dl_testing.env_params.generic import GenericEnvParamGetter

from bi_connector_metrica_tests.ext.config import CORE_TEST_CONFIG


def pytest_configure(config):  # noqa
    initialize_core_test(pytest_config=config, core_test_config=CORE_TEST_CONFIG)


@pytest.fixture(scope="session")
def env_param_getter() -> GenericEnvParamGetter:
    filepath = os.path.join(os.path.dirname(__file__), "params.yml")
    return GenericEnvParamGetter.from_yaml_file(filepath)


@pytest.fixture(scope="session")
def metrica_token(env_param_getter: GenericEnvParamGetter) -> str:
    return env_param_getter.get_str_value("METRIKA_OAUTH")
