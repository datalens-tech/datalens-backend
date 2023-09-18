from dl_core_testing.initialization import initialize_core_test

from dl_formula.loader import load_bi_formula
from dl_formula_testing.forced_literal import forced_literal_use

from bi_connector_yql_tests.db.config import CORE_TEST_CONFIG


def pytest_configure(config):  # noqa
    initialize_core_test(pytest_config=config, core_test_config=CORE_TEST_CONFIG)
    load_bi_formula()


__all__ = (
    # auto-use fixtures:
    'forced_literal_use',
)
