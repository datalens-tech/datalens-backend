from collections.abc import Generator

import pytest
import shortuuid
import sqlalchemy as sa

from dl_core.connection_executors.async_base import AsyncConnExecutorBase
from dl_core.connection_executors.common_base import ConnExecutorQuery
from dl_core_testing.database import Db
from dl_core_testing.testcases.connection_executor import (
    BaseConnectionExecutorTestClass,
    ReadWriteAdapterTestSuite,
)

from dl_connector_clickhouse.core.clickhouse.us_connection import ConnectionClickhouse
from dl_connector_clickhouse.core.clickhouse_base.exc import CHReadonlyUserError
import dl_connector_clickhouse_tests.db.config as test_config
from dl_connector_clickhouse_tests.db.core.base import BaseClickHouseTestClass


class _BaseCHReadWriteTestClass(BaseClickHouseTestClass):
    @pytest.fixture
    def rw_table_name(self, db: Db) -> Generator[str, None, None]:
        # ClickHouse needs an explicit engine; Memory is server-wide (survives pooled-executor
        # connection switches, unlike TEMPORARY).
        name = f"dl_rw_test_{shortuuid.uuid().lower()}"
        db.execute(f"CREATE TABLE {name} (id Int64) ENGINE = Memory")
        try:
            yield name
        finally:
            db.execute(f"DROP TABLE IF EXISTS {name}")


class TestClickHouseReadWrite(
    _BaseCHReadWriteTestClass,
    ReadWriteAdapterTestSuite[ConnectionClickhouse],
):
    # Writable `datalens` user (server profile `default`) + connection readonly=0 → the adapter
    # sends readonly=0 for allow_write queries, so writes physically execute.
    @pytest.fixture
    def connection_creation_params(self) -> dict:
        return {
            "db_name": test_config.CoreConnectionSettings.DB_NAME,
            "host": test_config.CoreConnectionSettings.HOST,
            "port": test_config.CoreConnectionSettings.PORT,
            "username": test_config.CoreConnectionSettings.USERNAME,
            "password": test_config.CoreConnectionSettings.PASSWORD,
            "readonly": 0,
        }

    # Override the shared negative test: ClickHouse rejects a readonly=2 write server-side with
    # code 164 → CHReadonlyUserError (not DatabaseReadOnlyTransactionError), and the error is raised by
    # `execute()` itself (non-200 response), not by `get_all()`.
    @pytest.mark.asyncio
    async def test_write_rejected_when_not_allow_write(
        self,
        async_connection_executor: AsyncConnExecutorBase,
        rw_table_name: str,
    ) -> None:
        query = ConnExecutorQuery(
            query=sa.text(f"INSERT INTO {rw_table_name} (id) VALUES (1)"),
            is_dashsql_query=True,
            autodetect_user_types=True,
            allow_write=False,
        )
        with pytest.raises(CHReadonlyUserError):
            await async_connection_executor.execute(query)

    # A readwrite SELECT (allow_write=True) must still return rows — proves the default_format read
    # path and the empty-body handling don't break result-set queries.
    @pytest.mark.asyncio
    async def test_readwrite_select_returns_rows(
        self,
        async_connection_executor: AsyncConnExecutorBase,
    ) -> None:
        query = ConnExecutorQuery(
            query=sa.text("SELECT 1"),
            is_dashsql_query=True,
            autodetect_user_types=True,
            allow_write=True,
        )
        result = await async_connection_executor.execute(query)
        rows = await result.get_all()
        assert [tuple(row) for row in rows] == [(1,)], rows


class TestClickHouseReadWriteReadonly2(
    _BaseCHReadWriteTestClass,
    BaseConnectionExecutorTestClass[ConnectionClickhouse],
):
    # Connection readonly=2 (the default): even with allow_write=True the adapter must send
    # readonly=2, so the write is rejected. Proves readonly=2 is honored on a readwrite connection.
    @pytest.fixture
    def connection_creation_params(self) -> dict:
        return {
            "db_name": test_config.CoreConnectionSettings.DB_NAME,
            "host": test_config.CoreConnectionSettings.HOST,
            "port": test_config.CoreConnectionSettings.PORT,
            "username": test_config.CoreConnectionSettings.USERNAME,
            "password": test_config.CoreConnectionSettings.PASSWORD,
            "readonly": 2,
        }

    @pytest.mark.asyncio
    async def test_write_rejected_when_readonly_2_even_with_allow_write(
        self,
        async_connection_executor: AsyncConnExecutorBase,
        rw_table_name: str,
    ) -> None:
        query = ConnExecutorQuery(
            query=sa.text(f"INSERT INTO {rw_table_name} (id) VALUES (1)"),
            is_dashsql_query=True,
            autodetect_user_types=True,
            allow_write=True,
        )
        with pytest.raises(CHReadonlyUserError):
            await async_connection_executor.execute(query)
