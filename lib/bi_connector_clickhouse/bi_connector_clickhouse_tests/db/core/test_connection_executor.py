from typing import Optional

import pytest

from bi_core.connection_models.common_models import DBIdent

from bi_core_testing.testcases.connection_executor import (
    DefaultSyncAsyncConnectionExecutorCheckBase,
    DefaultSyncConnectionExecutorTestSuite, DefaultAsyncConnectionExecutorTestSuite,
)

from bi_core.connectors.clickhouse.us_connection import ConnectionClickhouse

from bi_connector_clickhouse_tests.db.config import CoreConnectionSettings
from bi_connector_clickhouse_tests.db.core.base import BaseClickHouseTestClass


class ClickHouseSyncAsyncConnectionExecutorCheckBase(
        BaseClickHouseTestClass,
        DefaultSyncAsyncConnectionExecutorCheckBase[ConnectionClickhouse],
):
    do_check_closing_sql_sessions = False

    @pytest.fixture(scope='function')
    def db_ident(self) -> DBIdent:
        return DBIdent(db_name=CoreConnectionSettings.DB_NAME)

    def check_db_version(self, db_version: Optional[str]) -> None:
        assert db_version is not None
        assert '.' in db_version


class TestClickHouseSyncConnectionExecutor(
        ClickHouseSyncAsyncConnectionExecutorCheckBase,
        DefaultSyncConnectionExecutorTestSuite[ConnectionClickhouse],
):
    pass


class TestClickHouseAsyncConnectionExecutor(
        ClickHouseSyncAsyncConnectionExecutorCheckBase,
        DefaultAsyncConnectionExecutorTestSuite[ConnectionClickhouse],
):
    do_check_db_version = False
    do_check_test = False
    do_check_table_exists = False
    do_check_table_not_exists = False
