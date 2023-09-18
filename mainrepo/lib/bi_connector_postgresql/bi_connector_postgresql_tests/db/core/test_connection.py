from __future__ import annotations

import asyncio
import os
from typing import (
    TYPE_CHECKING,
    Callable,
    TypeVar,
)

import pytest

import bi_configs.utils as bi_configs_utils
from bi_core.us_connection_base import (
    ConnectionSQL,
    DataSourceTemplate,
)
from bi_core_testing.testcases.connection import (
    DefaultAsyncConnectionTestClass,
    DefaultConnectionTestClass,
)

from bi_connector_postgresql.core.postgresql.us_connection import ConnectionPostgreSQL
from bi_connector_postgresql_tests.db.core.base import (
    BasePostgreSQLTestClass,
    BaseSslPostgreSQLTestClass,
)

if TYPE_CHECKING:
    from bi_core.connection_executors import (
        AsyncConnExecutorBase,
        SyncConnExecutorBase,
    )


_CONN_TV = TypeVar("_CONN_TV", bound=ConnectionSQL)


class TestPostgreSQLConnection(
    BasePostgreSQLTestClass,
    DefaultConnectionTestClass[ConnectionPostgreSQL],
):
    do_check_data_export_flag = True

    def check_saved_connection(self, conn: ConnectionPostgreSQL, params: dict) -> None:
        assert conn.uuid is not None
        assert conn.data.db_name == params["db_name"]
        assert conn.data.host == params["host"]
        assert conn.data.port == params["port"]
        assert conn.data.username == params["username"]
        assert conn.data.password == params["password"]
        assert conn.data.ssl_enable is False
        assert conn.data.ssl_ca is None

    def check_data_source_templates(self, conn: ConnectionPostgreSQL, dsrc_templates: list[DataSourceTemplate]) -> None:
        assert dsrc_templates
        for dsrc_tmpl in dsrc_templates:
            assert dsrc_tmpl.title
            if dsrc_tmpl.parameters.get("schema_name") is not None:
                assert dsrc_tmpl.group == [dsrc_tmpl.parameters["schema_name"]]

    def test_get_data_source_templates_pg_partitioned(
        self, saved_connection, pg_partitioned_table_name, conn_default_service_registry
    ):
        connection = saved_connection
        service_registry = conn_default_service_registry
        templates = connection.get_data_source_templates(
            conn_executor_factory=service_registry.get_conn_executor_factory().get_sync_conn_executor,
        )
        names = [tpl.title for tpl in templates]
        assert pg_partitioned_table_name in names
        # # To reconsider later:
        # assert pg_partitioned_table + '_01' not in names


@pytest.mark.skipif(os.environ.get("WE_ARE_IN_CI"), reason="can't use localhost")
class TestSslPostgreSQLConnection(
    BaseSslPostgreSQLTestClass,
    DefaultConnectionTestClass[ConnectionPostgreSQL],
):
    def check_ssl_directory_is_empty(self) -> None:
        assert not os.listdir(bi_configs_utils.get_temp_root_certificates_folder_path())

    @pytest.fixture(autouse=True)
    def clear_ssl_folder(self):
        folder_path = bi_configs_utils.get_temp_root_certificates_folder_path()
        for filename in os.listdir(folder_path):
            file_path = os.path.join(folder_path, filename)
            os.unlink(file_path)

    def check_saved_connection(self, conn: ConnectionPostgreSQL, params: dict) -> None:
        assert conn.uuid is not None
        assert conn.data.db_name == params["db_name"]
        assert conn.data.host == params["host"]
        assert conn.data.port == params["port"]
        assert conn.data.username == params["username"]
        assert conn.data.password == params["password"]
        assert conn.data.ssl_enable is True
        assert conn.data.ssl_ca is params["ssl_ca"]

    def check_data_source_templates(self, conn: ConnectionPostgreSQL, dsrc_templates: list[DataSourceTemplate]) -> None:
        assert dsrc_templates
        for dsrc_tmpl in dsrc_templates:
            assert dsrc_tmpl.title

    def test_connection_test(
        self,
        saved_connection: ConnectionPostgreSQL,
        sync_conn_executor_factory: Callable[[], SyncConnExecutorBase],
    ) -> None:
        super().test_connection_test(saved_connection, sync_conn_executor_factory)
        self.check_ssl_directory_is_empty()


@pytest.mark.skipif(os.environ.get("WE_ARE_IN_CI"), reason="can't use localhost")
class TestSslAsyncPostgreSQLConnection(DefaultAsyncConnectionTestClass, TestSslPostgreSQLConnection):
    @pytest.mark.asyncio
    async def test_connection_test(
        self,
        saved_connection: ConnectionPostgreSQL,
        async_conn_executor_factory: Callable[[], AsyncConnExecutorBase],
    ) -> None:
        await super().test_connection_test(saved_connection, async_conn_executor_factory)
        await asyncio.sleep(0.1)
        self.check_ssl_directory_is_empty()

    @pytest.mark.asyncio
    async def test_multiple_connection_test(
        self,
        saved_connection: ConnectionPostgreSQL,
        async_conn_executor_factory: Callable[[], AsyncConnExecutorBase],
    ) -> None:
        await super().test_multiple_connection_test(saved_connection, async_conn_executor_factory)
        await asyncio.sleep(0.1)
        self.check_ssl_directory_is_empty()
