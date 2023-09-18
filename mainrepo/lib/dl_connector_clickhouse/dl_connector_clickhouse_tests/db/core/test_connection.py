from __future__ import annotations

import asyncio
import os
from typing import (
    TYPE_CHECKING,
    Callable,
)

import pytest

import dl_configs.utils as bi_configs_utils
from dl_connector_clickhouse.core.clickhouse.us_connection import ConnectionClickhouse
from dl_connector_clickhouse_tests.db.core.base import (
    BaseClickHouseTestClass,
    BaseSslClickHouseTestClass,
)
from dl_core.us_connection_base import DataSourceTemplate
from dl_core_testing.testcases.connection import (
    DefaultAsyncConnectionTestClass,
    DefaultConnectionTestClass,
)

if TYPE_CHECKING:
    from dl_core.connection_executors import (
        AsyncConnExecutorBase,
        SyncConnExecutorBase,
    )


class TestClickHouseConnection(
    BaseClickHouseTestClass,
    DefaultConnectionTestClass[ConnectionClickhouse],
):
    do_check_data_export_flag = True

    def check_saved_connection(self, conn: ConnectionClickhouse, params: dict) -> None:
        assert conn.uuid is not None
        assert conn.data.db_name == params["db_name"]
        assert conn.data.username == params["username"]
        assert conn.data.secure is False
        assert conn.data.ssl_ca is None

    def check_data_source_templates(
        self,
        conn: ConnectionClickhouse,
        dsrc_templates: list[DataSourceTemplate],
    ) -> None:
        assert dsrc_templates
        for dsrc_tmpl in dsrc_templates:
            assert conn.db_name in dsrc_tmpl.group
            assert dsrc_tmpl.title

        # Make sure
        tmpl_db_names = {dsrc_tmpl.group[0] for dsrc_tmpl in dsrc_templates}
        assert "system" not in tmpl_db_names


@pytest.mark.skipif(os.environ.get("WE_ARE_IN_CI"), reason="can't use localhost")
class TestSslClickHouseConnection(
    BaseSslClickHouseTestClass,
    DefaultConnectionTestClass[ConnectionClickhouse],
):
    def check_ssl_directory_is_empty(self) -> None:
        assert not os.listdir(bi_configs_utils.get_temp_root_certificates_folder_path())

    def check_saved_connection(self, conn: ConnectionClickhouse, params: dict) -> None:
        assert conn.uuid is not None
        assert conn.data.db_name == params["db_name"]
        assert conn.data.username == params["username"]
        assert conn.data.secure is True
        assert conn.data.ssl_ca is params["ssl_ca"]

    def check_data_source_templates(self, conn: ConnectionClickhouse, dsrc_templates: list[DataSourceTemplate]) -> None:
        assert dsrc_templates
        for dsrc_tmpl in dsrc_templates:
            assert dsrc_tmpl.title

    def test_connection_test(
        self,
        saved_connection: ConnectionClickhouse,
        sync_conn_executor_factory: Callable[[], SyncConnExecutorBase],
    ) -> None:
        super().test_connection_test(saved_connection, sync_conn_executor_factory)
        self.check_ssl_directory_is_empty()


@pytest.mark.skipif(os.environ.get("WE_ARE_IN_CI"), reason="can't use localhost")
class TestSslAsyncClickHouseConnection(DefaultAsyncConnectionTestClass, TestSslClickHouseConnection):
    @pytest.mark.asyncio
    async def test_connection_test(
        self,
        saved_connection: ConnectionClickhouse,
        async_conn_executor_factory: Callable[[], AsyncConnExecutorBase],
    ) -> None:
        await super().test_connection_test(saved_connection, async_conn_executor_factory)
        await asyncio.sleep(0.1)
        self.check_ssl_directory_is_empty()

    @pytest.mark.asyncio
    async def test_multiple_connection_test(
        self,
        saved_connection: ConnectionClickhouse,
        async_conn_executor_factory: Callable[[], AsyncConnExecutorBase],
    ) -> None:
        await super().test_multiple_connection_test(saved_connection, async_conn_executor_factory)
        await asyncio.sleep(0.1)
        self.check_ssl_directory_is_empty()
