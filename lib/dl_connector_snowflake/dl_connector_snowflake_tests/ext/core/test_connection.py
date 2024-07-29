import asyncio

from mock import MagicMock
import pytest

from dl_core.connection_executors.common_base import ConnExecutorQuery
from dl_core.us_connection_base import DataSourceTemplate
from dl_core_testing.testcases.connection import DefaultConnectionTestClass

from dl_connector_snowflake.auth import SFAuthProvider
from dl_connector_snowflake.core.exc import (
    SnowflakeAccessTokenError,
    SnowflakeGetAccessTokenError,
)
from dl_connector_snowflake.core.us_connection import ConnectionSQLSnowFlake
from dl_connector_snowflake_tests.ext.core.base import (
    BaseSnowFlakeTestClass,
    SnowFlakeTestClassWithExpiredRefreshToken,
    SnowFlakeTestClassWithRefreshTokenSoonToExpire,
)


class TestSnowFlakeConnection(
    BaseSnowFlakeTestClass,
    DefaultConnectionTestClass[ConnectionSQLSnowFlake],
):
    def check_saved_connection(self, conn: ConnectionSQLSnowFlake, params: dict) -> None:
        assert conn.uuid is not None

    def check_data_source_templates(
        self,
        conn: ConnectionSQLSnowFlake,
        dsrc_templates: list[DataSourceTemplate],
    ) -> None:
        pass  # TODO

    def test_connection_test(self, saved_connection, sync_conn_executor_factory) -> None:
        conn_executor = sync_conn_executor_factory()
        conn_executor.execute(ConnExecutorQuery(query="select 1"))


class TestSnowFlakeConnectionWithExpiredRefreshToken(
    TestSnowFlakeConnection,
    SnowFlakeTestClassWithExpiredRefreshToken,
):
    def test_connection_test(self, saved_connection, sync_conn_executor_factory) -> None:
        conn_executor = sync_conn_executor_factory()
        # note: currently could not get proper error message with aiohttp client
        # with pytest.raises(SnowflakeRefreshTokenInvalid):
        with pytest.raises(SnowflakeGetAccessTokenError):
            conn_executor.execute(ConnExecutorQuery(query="select 1"))


class TestSnowFlakeConnectionWithRefreshTokenSoonToExpire(
    TestSnowFlakeConnection,
    SnowFlakeTestClassWithRefreshTokenSoonToExpire,
):
    def test_connection_test(self, saved_connection, sync_conn_executor_factory, monkeypatch) -> None:
        conn_executor = sync_conn_executor_factory()

        future = asyncio.Future()
        future.set_result("bad_token")
        mck = MagicMock()
        mck.return_value = future

        monkeypatch.setattr(SFAuthProvider, "async_get_access_token", mck)
        with pytest.raises(SnowflakeAccessTokenError):
            conn_executor.execute(ConnExecutorQuery(query="select 1"))
