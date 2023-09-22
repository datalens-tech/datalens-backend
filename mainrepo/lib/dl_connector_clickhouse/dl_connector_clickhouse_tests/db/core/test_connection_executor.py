import asyncio
import os
from typing import Optional

import pytest

from dl_connector_clickhouse.core.clickhouse.us_connection import ConnectionClickhouse
from dl_connector_clickhouse_tests.db.config import CoreConnectionSettings
from dl_connector_clickhouse_tests.db.core.base import (
    BaseClickHouseTestClass,
    BaseSslClickHouseTestClass,
)
from dl_core.connection_executors import (
    AsyncConnExecutorBase,
    SyncConnExecutorBase,
)
from dl_core.connection_models.common_models import DBIdent
from dl_core_testing.testcases.connection_executor import (
    DefaultAsyncConnectionExecutorTestSuite,
    DefaultSyncAsyncConnectionExecutorCheckBase,
    DefaultSyncConnectionExecutorTestSuite,
)
from dl_testing.regulated_test import RegulatedTestParams


class ClickHouseSyncAsyncConnectionExecutorCheckBase(
    BaseClickHouseTestClass,
    DefaultSyncAsyncConnectionExecutorCheckBase[ConnectionClickhouse],
):
    test_params = RegulatedTestParams(
        mark_tests_skipped={
            DefaultAsyncConnectionExecutorTestSuite.test_closing_sql_sessions: "Not implemented",
        },
    )

    @pytest.fixture(scope="function")
    def db_ident(self) -> DBIdent:
        return DBIdent(db_name=CoreConnectionSettings.DB_NAME)

    def check_db_version(self, db_version: Optional[str]) -> None:
        assert db_version is not None
        assert "." in db_version


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
            DefaultAsyncConnectionExecutorTestSuite.test_get_db_version: "Not implemented",
            DefaultAsyncConnectionExecutorTestSuite.test_test: "Not implemented",
            DefaultAsyncConnectionExecutorTestSuite.test_table_exists: "Not implemented",
            DefaultAsyncConnectionExecutorTestSuite.test_table_not_exists: "Not implemented",
            DefaultAsyncConnectionExecutorTestSuite.test_get_table_schema_info: "Not implemented",
            DefaultAsyncConnectionExecutorTestSuite.test_get_table_schema_info_for_nonexistent_table: "Not implemented",
        },
    )


@pytest.mark.skipif(os.environ.get("WE_ARE_IN_CI"), reason="can't use localhost")
class TestSslClickHouseSyncConnectionExecutor(
    BaseSslClickHouseTestClass,
    TestClickHouseSyncConnectionExecutor,
):
    def test_test(self, sync_connection_executor: SyncConnExecutorBase) -> None:
        super().test_test(sync_connection_executor)
        self.check_ssl_directory_is_empty()


@pytest.mark.skipif(os.environ.get("WE_ARE_IN_CI"), reason="can't use localhost")
class TestSslClickHouseAsyncConnectionExecutor(
    BaseSslClickHouseTestClass,
    TestClickHouseAsyncConnectionExecutor,
):
    async def test_test(self, async_connection_executor: AsyncConnExecutorBase) -> None:
        await super().test_test(async_connection_executor)
        await asyncio.sleep(0.1)
        self.check_ssl_directory_is_empty()

    async def test_multiple_connection_test(self, async_connection_executor: AsyncConnExecutorBase) -> None:
        await super().test_multiple_connection_test(async_connection_executor)
        await asyncio.sleep(0.1)
        self.check_ssl_directory_is_empty()
