from dl_api_lib_testing.initialization import initialize_api_lib_test
from dl_formula_testing.forced_literal import forced_literal_use
import dl_sqlalchemy_ydb.dialect

from dl_connector_ydb_tests.db.config import API_TEST_CONFIG


def pytest_configure(config):  # noqa
    initialize_api_lib_test(pytest_config=config, api_test_config=API_TEST_CONFIG)

    dl_sqlalchemy_ydb.dialect.register_dialect()


__all__ = (
    # auto-use fixtures:
    "forced_literal_use",
)
