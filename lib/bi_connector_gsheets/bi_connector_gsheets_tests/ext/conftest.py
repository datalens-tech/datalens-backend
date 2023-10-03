import os

import pytest

from bi_testing_ya.tvm_info import TvmSecretReader
from dl_api_lib_testing.initialization import initialize_api_lib_test
from dl_testing.env_params.generic import GenericEnvParamGetter

from bi_connector_gsheets_tests.ext.config import API_TEST_CONFIG


pytest_plugins = ("aiohttp.pytest_plugin",)  # and it, in turn, includes 'pytest_asyncio.plugin'


def pytest_configure(config):  # noqa
    initialize_api_lib_test(pytest_config=config, api_test_config=API_TEST_CONFIG)


@pytest.fixture(scope="session")
def env_param_getter() -> GenericEnvParamGetter:
    filepath = os.path.join(os.path.dirname(__file__), "params.yml")
    return GenericEnvParamGetter.from_yaml_file(filepath)


@pytest.fixture(scope="session")
def tvm_secret_reader(env_param_getter) -> TvmSecretReader:
    return TvmSecretReader(env_param_getter)


@pytest.fixture(scope="session", autouse=True)
def tvm_info(tvm_secret_reader) -> str:
    tvm_info_str = tvm_secret_reader.get_tvm_info_str()
    os.environ["TVM_INFO"] = tvm_info_str
    return tvm_info_str
