from dl_core_testing.initialization import initialize_core_test
from dl_core_tests.db.config import CORE_TEST_CONFIG


def pytest_configure(config):  # noqa
    initialize_core_test(pytest_config=config, core_test_config=CORE_TEST_CONFIG)
