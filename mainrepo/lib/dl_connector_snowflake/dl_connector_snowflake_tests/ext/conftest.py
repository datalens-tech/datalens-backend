import os

import pytest

from dl_api_lib.loader import load_api_lib
from dl_api_lib_testing.initialization import initialize_api_lib_test
from dl_connector_snowflake.core.testing.secrets import SnowFlakeSecretReader
from dl_connector_snowflake_tests.ext.config import API_TEST_CONFIG
from dl_formula_testing.forced_literal import forced_literal_use
from dl_testing.env_params.generic import GenericEnvParamGetter


pytest_plugins = ("aiohttp.pytest_plugin",)  # and it, in turn, includes 'pytest_asyncio.plugin'


def pytest_configure(config):  # noqa
    initialize_api_lib_test(pytest_config=config, api_test_config=API_TEST_CONFIG)


@pytest.fixture(scope="session")
def env_param_getter() -> GenericEnvParamGetter:
    filepath = os.path.join(os.path.dirname(__file__), "params.yml")
    return GenericEnvParamGetter.from_yaml_file(filepath)


@pytest.fixture(scope="session")
def sf_secrets(env_param_getter):
    return SnowFlakeSecretReader(env_param_getter)


__all__ = (
    # auto-use fixtures:
    "forced_literal_use",
)
