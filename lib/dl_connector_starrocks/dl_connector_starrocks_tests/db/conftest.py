import logging

import pymysql
import pytest

from dl_api_lib_testing.initialization import initialize_api_lib_test
from dl_formula_testing.forced_literal import forced_literal_use

from dl_connector_starrocks_tests.db.config import (
    API_TEST_CONFIG,
    CoreConnectionSettings,
)


LOGGER = logging.getLogger(__name__)


def _initialize_starrocks_db() -> None:
    """Create the test database in StarRocks if it doesn't exist.

    StarRocks (unlike MySQL) does not support a MYSQL_DATABASE env var
    or docker-entrypoint-initdb.d scripts, so we create it from the test harness.
    """
    conn = pymysql.connect(
        host=CoreConnectionSettings.HOST,
        port=CoreConnectionSettings.PORT,
        user=CoreConnectionSettings.USERNAME,
        password=CoreConnectionSettings.PASSWORD,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(f"CREATE DATABASE IF NOT EXISTS `{CoreConnectionSettings.DB_NAME}`")
        LOGGER.info("Ensured StarRocks database '%s' exists", CoreConnectionSettings.DB_NAME)
    finally:
        conn.close()


def pytest_configure(config: pytest.Config) -> None:
    _initialize_starrocks_db()
    initialize_api_lib_test(pytest_config=config, api_test_config=API_TEST_CONFIG)


__all__ = ("forced_literal_use",)  # auto-use fixture
