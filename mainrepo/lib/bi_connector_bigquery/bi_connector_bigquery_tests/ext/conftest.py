import os

import pytest

from bi_core_testing.initialization import initialize_core_test

from bi_api_lib.loader import load_bi_api_lib
from bi_formula.testing.forced_literal import forced_literal_use

from bi_connector_bigquery.testing.secrets import BigQuerySecretReaderBase, BigQuerySecretReader

from bi_testing.env_params.generic import GenericEnvParamGetter
from bi_testing.files import get_file_loader

from bi_connector_bigquery_tests.ext.config import CORE_TEST_CONFIG


pytest_plugins = (
    'aiohttp.pytest_plugin',  # and it, in turn, includes 'pytest_asyncio.plugin'
)


def pytest_configure(config):  # noqa
    initialize_core_test(pytest_config=config, core_test_config=CORE_TEST_CONFIG)
    load_bi_api_lib()


@pytest.fixture(scope='session')
def env_param_getter() -> GenericEnvParamGetter:
    filepath = os.path.join(os.path.dirname(__file__), 'params.yml')
    filepath = get_file_loader().resolve_path(filepath)
    return GenericEnvParamGetter.from_yaml_file(filepath)


@pytest.fixture(scope='session')
def bq_secrets(env_param_getter) -> BigQuerySecretReaderBase:
    return BigQuerySecretReader(env_param_getter=env_param_getter)


__all__ = (
    # auto-use fixtures:
    'forced_literal_use',
)
