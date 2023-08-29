from typing import Optional

import pytest

from bi_core.connection_models.common_models import DBIdent

from bi_testing.regulated_test import RegulatedTestParams
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
    test_params = RegulatedTestParams(
        mark_tests_skipped={
            DefaultAsyncConnectionExecutorTestSuite.test_closing_sql_sessions: 'Not implemented',
        },
    )

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
    test_params = RegulatedTestParams(
        mark_tests_skipped={
            DefaultAsyncConnectionExecutorTestSuite.test_get_db_version: 'Not implemented',
            DefaultAsyncConnectionExecutorTestSuite.test_test: 'Not implemented',
            DefaultAsyncConnectionExecutorTestSuite.test_table_exists: 'Not implemented',
            DefaultAsyncConnectionExecutorTestSuite.test_table_not_exists: 'Not implemented',
            DefaultAsyncConnectionExecutorTestSuite.test_get_table_schema_info: 'Not implemented',
            DefaultAsyncConnectionExecutorTestSuite.test_get_table_schema_info_for_nonexistent_table: 'Not implemented',
        },
    )
