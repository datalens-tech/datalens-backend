from __future__ import annotations

import os
from typing import TypeVar

import pytest

from dl_core.us_connection_base import (
    ConnectionSQL,
    DataSourceTemplate,
)
from dl_core_testing.testcases.connection import DefaultConnectionTestClass

from dl_connector_postgresql.core.postgresql.us_connection import ConnectionPostgreSQL
from dl_connector_postgresql_tests.db.core.base import (
    BasePostgreSQLTestClass,
    BaseSslPostgreSQLTestClass,
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

    def test_get_tables(self, saved_connection: ConnectionPostgreSQL, sync_conn_executor_factory) -> None:
        conn = saved_connection

        def sync_conn_executor_factory_for_conn(connection):
            return sync_conn_executor_factory()

        tables = conn.get_tables(sync_conn_executor_factory_for_conn)
        assert tables

    def test_get_parameter_combinations(
        self, saved_connection: ConnectionPostgreSQL, sync_conn_executor_factory
    ) -> None:
        conn = saved_connection

        def sync_conn_executor_factory_for_conn(connection):
            return sync_conn_executor_factory()

        # test search
        param_combinations = conn.get_parameter_combinations(sync_conn_executor_factory_for_conn, search_text="ampl")
        assert param_combinations
        assert all("ampl" in i["table_name"] for i in param_combinations)

        # Test with offset
        offset = 2
        param_combinations_with_offset = conn.get_parameter_combinations(
            sync_conn_executor_factory_for_conn,
            offset=offset,
        )
        param_combinations_without_offset = conn.get_parameter_combinations(
            sync_conn_executor_factory_for_conn,
        )
        if len(param_combinations_without_offset) >= offset:
            assert len(param_combinations_without_offset) == len(param_combinations_with_offset) + offset
        else:
            assert len(param_combinations_without_offset) >= len(param_combinations_with_offset) == 0

        # Test with limit
        param_combinations = conn.get_parameter_combinations(
            sync_conn_executor_factory_for_conn,
            limit=1,
        )
        assert len(param_combinations) == 1

    def test_get_source_templates_paginated(
        self, saved_connection: ConnectionPostgreSQL, sync_conn_executor_factory
    ) -> None:
        conn = saved_connection

        def sync_conn_executor_factory_for_conn(connection):
            return sync_conn_executor_factory()

        # Test basic functionality compared to the old method
        templates = conn.get_data_source_templates_paginated(sync_conn_executor_factory_for_conn)
        old_templates = conn.get_data_source_templates(sync_conn_executor_factory_for_conn)
        assert templates == old_templates

        # Test search functionality
        templates_with_offset = conn.get_data_source_templates_paginated(
            sync_conn_executor_factory_for_conn, search_text="ampl", limit=1, offset=1
        )
        templates_with_only_limit = conn.get_data_source_templates_paginated(
            sync_conn_executor_factory_for_conn,
            search_text="ampl",
            limit=2,
        )
        assert len(templates_with_offset) == len(templates_with_only_limit) - 1
        assert templates_with_offset == templates_with_only_limit[1:]
        assert all("ampl" in template.title for template in templates_with_only_limit)

        # Test search with non-existent pattern
        templates = conn.get_data_source_templates_paginated(
            sync_conn_executor_factory_for_conn,
            search_text="non_existent_table",
        )
        assert len(templates) == 0

        # Test with None and empty search_text (should be equivalent to no search)
        templates_none = conn.get_data_source_templates_paginated(sync_conn_executor_factory_for_conn, search_text=None)
        templates_no_search = conn.get_data_source_templates_paginated(sync_conn_executor_factory_for_conn)
        templates_empty_search = conn.get_data_source_templates_paginated(
            sync_conn_executor_factory_for_conn, search_text=""
        )
        assert templates_none == templates_no_search == templates_empty_search


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
