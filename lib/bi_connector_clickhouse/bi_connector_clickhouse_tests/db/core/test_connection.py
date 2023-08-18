import asyncio
import os

import pytest

import bi_configs.utils as bi_configs_utils
from bi_core.us_connection_base import DataSourceTemplate
from bi_core_testing.testcases.connection import DefaultConnectionTestClass

from bi_core.connectors.clickhouse.us_connection import ConnectionClickhouse

from bi_connector_clickhouse_tests.db.core.base import BaseClickHouseTestClass, BaseSslClickHouseTestClass


class TestClickHouseConnection(
        BaseClickHouseTestClass,
        DefaultConnectionTestClass[ConnectionClickhouse],
):
    do_check_data_export_flag = True

    def check_saved_connection(self, conn: ConnectionClickhouse, params: dict) -> None:
        assert conn.uuid is not None
        assert conn.data.db_name == params['db_name']
        assert conn.data.username == params['username']
        assert conn.data.secure is False
        assert conn.data.ssl_ca is None

    def check_data_source_templates(
            self, conn: ConnectionClickhouse, dsrc_templates: list[DataSourceTemplate],
    ) -> None:
        assert dsrc_templates
        for dsrc_tmpl in dsrc_templates:
            assert conn.db_name in dsrc_tmpl.group
            assert dsrc_tmpl.title


# TODO: turn on in https://st.yandex-team.ru/BI-4738
@pytest.mark.skip
class TestSslClickHouseConnection(
    BaseSslClickHouseTestClass,
    DefaultConnectionTestClass[ConnectionClickhouse],
):
    def check_ssl_directory_is_empty(self) -> None:
        assert not os.listdir(bi_configs_utils.get_temp_root_certificates_folder_path())

    def check_saved_connection(self, conn: ConnectionClickhouse, params: dict) -> None:
        assert conn.uuid is not None
        assert conn.data.db_name == params['db_name']
        assert conn.data.username == params['username']
        assert conn.data.secure is True
        assert conn.data.ssl_ca is params['ssl_ca']

    def check_data_source_templates(self, conn: ConnectionClickhouse, dsrc_templates: list[DataSourceTemplate]) -> None:
        assert dsrc_templates
        for dsrc_tmpl in dsrc_templates:
            assert dsrc_tmpl.title


# TODO: turn on in https://st.yandex-team.ru/BI-4738
@pytest.mark.skip
class TestSslAsyncPostgreSQLConnection(TestSslClickHouseConnection):
    @pytest.fixture(scope='session')
    def conn_exec_factory_async_env(self) -> bool:
        return True

    @pytest.mark.asyncio
    async def test_connection_test(self, saved_connection: ConnectionClickhouse) -> None:
        conn = saved_connection
        await conn.async_conn_executor.test()
        await asyncio.sleep(0.1)

    @pytest.mark.asyncio
    async def test_multiple_connection_test(self, saved_connection: ConnectionClickhouse) -> None:
        conn = saved_connection
        coros = [conn.async_conn_executor.test() for _ in range(10)]
        await asyncio.gather(*coros)
        await asyncio.sleep(0.1)

    @pytest.mark.skip
    def test_connection_get_data_source_templates(self, saved_connection: ConnectionClickhouse) -> None:
        ...
