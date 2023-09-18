from dl_core_testing.initialization import initialize_core_test

from bi_connector_usage_tracking_ya_team_tests.db.config import CORE_TEST_CONFIG

def pytest_configure(config):  # noqa
    initialize_core_test(pytest_config=config, core_test_config=CORE_TEST_CONFIG)
