import os

import pytest

from bi_testing.env_params.generic import GenericEnvParamGetter
from bi_testing.files import get_file_loader

from bi_core_testing.initialization import initialize_core_test

from bi_connector_bundle_ch_filtered_ya_cloud_tests.ext.config import CORE_TEST_CONFIG


def pytest_configure(config):  # noqa
    initialize_core_test(pytest_config=config, core_test_config=CORE_TEST_CONFIG)


@pytest.fixture(scope='session')
def env_param_getter() -> GenericEnvParamGetter:
    filepath = os.path.join(os.path.dirname(__file__), 'params.yml')
    filepath = get_file_loader().resolve_path(filepath)
    return GenericEnvParamGetter.from_yaml_file(filepath)


@pytest.fixture(scope='session')
def ext_test_blackbox_user_oauth(env_param_getter):
    """
    User yndx-datalens-test
    Token received at https://oauth-test.yandex.ru/
    """
    return env_param_getter.get_str_value('EXT_TEST_BLACKBOX_USER_OAUTH')
