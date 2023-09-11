import os

import pytest

from bi_api_lib.loader import ApiLibraryConfig, load_bi_api_lib

from bi_core_testing.initialization import initialize_core_test

from bi_testing.env_params.generic import GenericEnvParamGetter

from bi_formula.loader import load_bi_formula
from bi_formula_testing.forced_literal import forced_literal_use

from bi_connector_snowflake.core.testing.secrets import SnowFlakeSecretReader

from bi_connector_snowflake_tests.ext.config import BI_TEST_CONFIG, CORE_TEST_CONFIG


pytest_plugins = (
    'aiohttp.pytest_plugin',  # and it, in turn, includes 'pytest_asyncio.plugin'
)


def pytest_configure(config):  # noqa
    load_bi_api_lib(ApiLibraryConfig(api_connector_ep_names=BI_TEST_CONFIG.bi_api_connector_whitelist))
    initialize_core_test(pytest_config=config, core_test_config=CORE_TEST_CONFIG)
    load_bi_formula()


@pytest.fixture(scope='session')
def env_param_getter() -> GenericEnvParamGetter:
    filepath = os.path.join(os.path.dirname(__file__), 'params.yml')
    return GenericEnvParamGetter.from_yaml_file(filepath)


@pytest.fixture(scope='session')
def sf_secrets(env_param_getter):
    return SnowFlakeSecretReader(env_param_getter)


__all__ = (
    # auto-use fixtures:
    'forced_literal_use',
)
