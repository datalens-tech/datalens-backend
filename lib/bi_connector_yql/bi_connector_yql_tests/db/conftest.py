import pytest

from bi_core_testing.initialization import initialize_core_test

from bi_formula.loader import load_bi_formula
from bi_formula_testing.forced_literal import forced_literal_use

from bi_connector_yql_tests.db.config import CORE_TEST_CONFIG


def pytest_configure(config):  # noqa
    initialize_core_test(pytest_config=config, core_test_config=CORE_TEST_CONFIG)
    load_bi_formula()


@pytest.fixture(scope='session', autouse=True)
def _register_dialects():
    """
    In arcadia tier0, entrypoint-register does not work, so do it manually for
    custom dialects.
    """
    from sqlalchemy.dialects import registry
    registry.register(
        "yql", "ydb.sqlalchemy", "YqlDialect")


__all__ = (
    # auto-use fixtures:
    'forced_literal_use',
)
