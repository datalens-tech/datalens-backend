import asyncio

from mock import MagicMock
import pytest

from dl_connector_snowflake.auth import SFAuthProvider
from dl_connector_snowflake.core.exc import (
    SnowflakeAccessTokenError,
    SnowflakeGetAccessTokenError,
)
from dl_connector_snowflake.core.us_connection import ConnectionSQLSnowFlake
from dl_connector_snowflake_tests.ext.core.base import BaseSnowFlakeTestClass  # noqa
from dl_core.connection_executors.common_base import ConnExecutorQuery
from dl_core.services_registry.top_level import ServicesRegistry
from dl_core.us_connection_base import DataSourceTemplate
from dl_core_testing.testcases.connection import DefaultConnectionTestClass


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
        pass

    def test_connection_with_correct_refresh_token(
        self,
        saved_connection: ConnectionSQLSnowFlake,
        conn_sync_service_registry: ServicesRegistry,
    ):
        ce_factory = conn_sync_service_registry.get_conn_executor_factory()
        conn_executor = ce_factory.get_sync_conn_executor(saved_connection)
        conn_executor.execute(ConnExecutorQuery(query="select 1"))

    def test_connection_test_with_expired_refresh_token(
        self,
        saved_connection_with_expired_refresh_token: ConnectionSQLSnowFlake,
        conn_sync_service_registry: ServicesRegistry,
    ):
        ce_factory = conn_sync_service_registry.get_conn_executor_factory()
        conn_executor = ce_factory.get_sync_conn_executor(saved_connection_with_expired_refresh_token)
        # note: currently could not get proper error message with aiohttp client
        # with pytest.raises(SnowflakeRefreshTokenInvalid):
        with pytest.raises(SnowflakeGetAccessTokenError):
            conn_executor.execute(ConnExecutorQuery(query="select 1"))

    def test_bad_access_token(
        self,
        saved_connection,
        conn_sync_service_registry: ServicesRegistry,
        monkeypatch,
    ):
        ce_factory = conn_sync_service_registry.get_conn_executor_factory()
        conn_executor = ce_factory.get_sync_conn_executor(saved_connection)

        future = asyncio.Future()
        future.set_result("bad_token")
        mck = MagicMock()
        mck.return_value = future

        monkeypatch.setattr(SFAuthProvider, "async_get_access_token", mck)
        with pytest.raises(SnowflakeAccessTokenError):
            conn_executor.execute(ConnExecutorQuery(query="select 1"))
