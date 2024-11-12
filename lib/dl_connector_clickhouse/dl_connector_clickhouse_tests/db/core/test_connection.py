from __future__ import annotations

import os

import pytest

from dl_core.us_connection_base import DataSourceTemplate
from dl_core_testing.testcases.connection import DefaultConnectionTestClass

from dl_connector_clickhouse.core.clickhouse.us_connection import ConnectionClickhouse
from dl_connector_clickhouse_tests.db.core.base import (
    BaseClickHouseDefaultUserTestClass,
    BaseClickHouseReadonlyUserTestClass,
    BaseClickHouseTestClass,
    BaseSslClickHouseTestClass,
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


class TestClickHouseDefaultUserConnection(BaseClickHouseDefaultUserTestClass, TestClickHouseConnection):
    def check_saved_connection(self, conn: ConnectionClickhouse, params: dict) -> None:
        assert conn.uuid is not None
        assert conn.data.db_name == params["db_name"]
        assert conn.data.username is None
        assert conn.data.secure is False
        assert conn.data.ssl_ca is None


class TestClickHouseReadonlyUserConnection(BaseClickHouseReadonlyUserTestClass, TestClickHouseConnection):
    def check_saved_connection(self, conn: ConnectionClickhouse, params: dict) -> None:
        assert conn.uuid is not None
        assert conn.data.db_name == params["db_name"]
        assert conn.data.username == params["username"]
        assert conn.data.secure is False
        assert conn.data.ssl_ca is None
        assert conn.data.readonly == 1


@pytest.mark.skipif(os.environ.get("WE_ARE_IN_CI"), reason="can't use localhost")
class TestSslClickHouseConnection(
    BaseSslClickHouseTestClass,
    TestClickHouseConnection,
    DefaultConnectionTestClass[ConnectionClickhouse],
):
    def check_saved_connection(self, conn: ConnectionClickhouse, params: dict) -> None:
        assert conn.uuid is not None
        assert conn.data.db_name == params["db_name"]
        assert conn.data.username == params["username"]
        assert conn.data.secure is True
        assert conn.data.ssl_ca is params["ssl_ca"]
