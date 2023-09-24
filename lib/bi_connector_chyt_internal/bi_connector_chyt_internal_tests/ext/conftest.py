import asyncio
import os

import aiohttp.pytest_plugin
import aiohttp.test_utils
import pytest

from dl_api_lib_testing.initialization import initialize_api_lib_test
from dl_testing.env_params.generic import GenericEnvParamGetter

from bi_connector_chyt_internal_tests.ext.config import BI_TEST_CONFIG


pytest_plugins = ("aiohttp.pytest_plugin",)

try:
    del aiohttp.pytest_plugin.loop
except AttributeError:
    pass


def pytest_configure(config):  # noqa
    initialize_api_lib_test(pytest_config=config, api_test_config=BI_TEST_CONFIG)


@pytest.fixture
def loop(event_loop):
    asyncio.set_event_loop(event_loop)
    yield event_loop
    # Attempt to cover an old version of pytest-asyncio:
    # https://github.com/pytest-dev/pytest-asyncio/commit/51d986cec83fdbc14fa08015424c79397afc7ad9
    asyncio.set_event_loop_policy(None)


@pytest.fixture(scope="session")
def env_param_getter() -> GenericEnvParamGetter:
    filepath = os.path.join(os.path.dirname(__file__), "params.yml")
    return GenericEnvParamGetter.from_yaml_file(filepath)


@pytest.fixture(scope="session")
def yt_token(env_param_getter: GenericEnvParamGetter) -> str:
    return env_param_getter.get_str_value("YT_OAUTH")
