from pytest import Config

from dl_api_lib.loader import load_bi_api_lib
from dl_api_lib_testing.configuration import BiApiTestEnvironmentConfiguration
from dl_core_testing.initialization import initialize_core_test


def initialize_api_lib_test(pytest_config: Config, api_test_config: BiApiTestEnvironmentConfiguration) -> None:
    load_bi_api_lib(api_lib_config=api_test_config.get_api_library_config())
    initialize_core_test(pytest_config=pytest_config, core_test_config=api_test_config.core_test_config)
