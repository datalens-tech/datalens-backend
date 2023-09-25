from __future__ import annotations

import os
from typing import TypeVar

import pytest

from dl_connector_postgresql.core.postgresql.us_connection import ConnectionPostgreSQL
from dl_connector_postgresql_tests.db.core.base import (
    BasePostgreSQLTestClass,
    BaseSslPostgreSQLTestClass,
)
from dl_core.us_connection_base import (
    ConnectionSQL,
    DataSourceTemplate,
)
from dl_core_testing.testcases.connection import DefaultConnectionTestClass


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
