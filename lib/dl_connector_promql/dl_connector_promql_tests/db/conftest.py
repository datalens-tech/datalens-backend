from dl_api_lib_testing.initialization import initialize_api_lib_test

from dl_connector_promql_tests.db.config import API_TEST_CONFIG


def pytest_configure(config):  # noqa
    initialize_api_lib_test(pytest_config=config, api_test_config=API_TEST_CONFIG)
