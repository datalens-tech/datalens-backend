import os

import pytest

from bi_testing_ya.tvm_info import TvmSecretReader
from dl_api_lib_testing.initialization import initialize_api_lib_test
from dl_testing.env_params.generic import GenericEnvParamGetter

from bi_connector_solomon_tests.ext.config import API_TEST_CONFIG


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


@pytest.fixture(scope="session")
def int_cookie(env_param_getter) -> str:
    import requests

    password = env_param_getter.get_str_value("ROBOT_STATBOX_KAPPA_PASSWORD")

    url = "https://passport.yandex-team.ru/auth"
    payload = f"login=robot-statbox-kappa&passwd={password}"
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
    }

    with requests.Session() as s:
        s.request("POST", url, headers=headers, data=payload)
        cookies = s.cookies.get_dict()

    return "; ".join(f"{k}={v}" for k, v in cookies.items())
