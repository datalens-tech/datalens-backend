import os

import pytest

from dl_api_lib.loader import (
    ApiLibraryConfig,
    load_bi_api_lib,
)
from dl_connector_bigquery.testing.secrets import (
    BigQuerySecretReader,
    BigQuerySecretReaderBase,
)
from dl_connector_bigquery_tests.ext.config import (
    BI_TEST_CONFIG,
    CORE_TEST_CONFIG,
)
from dl_core.loader import CoreLibraryConfig
from dl_core_testing.initialization import initialize_core_test
from dl_formula_testing.forced_literal import forced_literal_use
from dl_testing.env_params.generic import GenericEnvParamGetter


pytest_plugins = ("aiohttp.pytest_plugin",)  # and it, in turn, includes 'pytest_asyncio.plugin'


def pytest_configure(config):  # noqa
    load_bi_api_lib(
        ApiLibraryConfig(
            api_connector_ep_names=BI_TEST_CONFIG.bi_api_connector_whitelist,
            core_lib_config=CoreLibraryConfig(core_connector_ep_names=BI_TEST_CONFIG.core_connector_whitelist),
        )
    )
    initialize_core_test(pytest_config=config, core_test_config=CORE_TEST_CONFIG)


@pytest.fixture(scope="session")
def env_param_getter() -> GenericEnvParamGetter:
    filepath = os.path.join(os.path.dirname(__file__), "params.yml")
    return GenericEnvParamGetter.from_yaml_file(filepath)


@pytest.fixture(scope="session")
def bq_secrets(env_param_getter) -> BigQuerySecretReaderBase:
    return BigQuerySecretReader(env_param_getter=env_param_getter)


__all__ = (
    # auto-use fixtures:
    "forced_literal_use",
)
