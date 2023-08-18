from typing import Optional

import pytest

from bi_core.connection_models.common_models import DBIdent

from bi_core_testing.testcases.connection_executor import (
    DefaultSyncAsyncConnectionExecutorCheckBase,
    DefaultSyncConnectionExecutorTestSuite, DefaultAsyncConnectionExecutorTestSuite,
)

from bi_connector_mysql.core.us_connection import ConnectionMySQL

from bi_connector_mysql_tests.db.config import CoreConnectionSettings
from bi_connector_mysql_tests.db.core.base import BaseMySQLTestClass


class MySQLSyncAsyncConnectionExecutorCheckBase(
        BaseMySQLTestClass,
        DefaultSyncAsyncConnectionExecutorCheckBase[ConnectionMySQL],
):
    do_check_closing_sql_sessions = False  # FIXME

    @pytest.fixture(scope='function')
    def db_ident(self) -> DBIdent:
        return DBIdent(db_name=CoreConnectionSettings.DB_NAME)

    def check_db_version(self, db_version: Optional[str]) -> None:
        assert db_version is not None
        assert '.' in db_version


class TestMySQLSyncConnectionExecutor(
        MySQLSyncAsyncConnectionExecutorCheckBase,
        DefaultSyncConnectionExecutorTestSuite[ConnectionMySQL],
):
    pass


class TestMySQLAsyncConnectionExecutor(
        MySQLSyncAsyncConnectionExecutorCheckBase,
        DefaultAsyncConnectionExecutorTestSuite[ConnectionMySQL],
):
    do_check_table_exists = False
    do_check_table_not_exists = False
