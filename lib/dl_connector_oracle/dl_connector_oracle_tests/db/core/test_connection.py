from typing import Callable

import pytest
import requests

from dl_core.exc import DatabaseOperationalError
from dl_core.us_connection_base import (
    ConnectionBase,
    DataSourceTemplate,
)
from dl_core_testing.testcases.connection import DefaultConnectionTestClass

from dl_connector_oracle.core.connection_executors import OracleDefaultConnExecutor
from dl_connector_oracle.core.us_connection import ConnectionSQLOracle
from dl_connector_oracle_tests.db.config import (
    DEFAULT_ORACLE_SCHEMA_NAME,
    CoreSSLConnectionSettings,
)
from dl_connector_oracle_tests.db.core.base import (
    BaseOracleTestClass,
    BaseSSLOracleTestClass,
)


class TestOracleConnection(
    BaseOracleTestClass,
    DefaultConnectionTestClass[ConnectionSQLOracle],
):
    do_check_data_export_flag = True

    def check_saved_connection(self, conn: ConnectionSQLOracle, params: dict) -> None:
        assert conn.uuid is not None
        assert conn.data.db_name == params["db_name"]
        assert conn.data.host == params["host"]
        assert conn.data.port == params["port"]
        assert conn.data.username == params["username"]
        assert conn.data.password == params["password"]
        assert conn.data.ssl_enable is False
        assert conn.data.ssl_ca is None

    def check_data_source_templates(
        self,
        conn: ConnectionSQLOracle,
        dsrc_templates: list[DataSourceTemplate],
    ) -> None:
        assert dsrc_templates
        for dsrc_tmpl in dsrc_templates:
            assert dsrc_tmpl.title
            if dsrc_tmpl.parameters.get("schema_name") is not None:
                assert dsrc_tmpl.group == [dsrc_tmpl.parameters["schema_name"]]

        schema_names = {tmpl.parameters.get("schema_name") for tmpl in dsrc_templates}
        assert DEFAULT_ORACLE_SCHEMA_NAME in schema_names

    def test_get_source_templates_paginated(
        self,
        saved_connection: ConnectionSQLOracle,
        sync_conn_executor_factory: Callable[[], OracleDefaultConnExecutor],
    ) -> None:
        conn = saved_connection

        def sync_conn_executor_factory_for_conn(_connection: ConnectionBase) -> OracleDefaultConnExecutor:
            return sync_conn_executor_factory()

        templates = conn.get_data_source_templates_paginated(sync_conn_executor_factory_for_conn)
        old_templates = conn.get_data_source_templates(sync_conn_executor_factory_for_conn)
        assert templates == old_templates
        assert len(templates) > 1, "fixture must provide >1 source for meaningful pagination test"

        search_prefix = templates[0].title[:3]
        assert search_prefix

        templates_with_offset = conn.get_data_source_templates_paginated(
            sync_conn_executor_factory_for_conn, search_text=search_prefix, limit=1, offset=1
        )
        templates_with_only_limit = conn.get_data_source_templates_paginated(
            sync_conn_executor_factory_for_conn, search_text=search_prefix, limit=2
        )
        assert len(templates_with_offset) == len(templates_with_only_limit) - 1
        assert templates_with_offset == templates_with_only_limit[1:]
        assert all(search_prefix.lower() in tmpl.title.lower() for tmpl in templates_with_only_limit)

        templates_empty = conn.get_data_source_templates_paginated(
            sync_conn_executor_factory_for_conn,
            search_text="__no_such_table_xyz__",
        )
        assert len(templates_empty) == 0

        t_none = conn.get_data_source_templates_paginated(sync_conn_executor_factory_for_conn, search_text=None)
        t_empty = conn.get_data_source_templates_paginated(sync_conn_executor_factory_for_conn, search_text="")
        t_default = conn.get_data_source_templates_paginated(sync_conn_executor_factory_for_conn)
        assert t_none == t_empty == t_default

        templates_lim_one = conn.get_data_source_templates_paginated(sync_conn_executor_factory_for_conn, limit=1)
        assert len(templates_lim_one) == 1

    def test_get_tables_in_schema_paginated(
        self,
        saved_connection: ConnectionSQLOracle,
        sync_conn_executor_factory: Callable[[], OracleDefaultConnExecutor],
    ) -> None:
        conn = saved_connection

        def sync_conn_executor_factory_for_conn(_connection: ConnectionBase) -> OracleDefaultConnExecutor:
            return sync_conn_executor_factory()

        all_tables = conn.get_tables(
            conn_executor_factory=sync_conn_executor_factory_for_conn,
            schema_name=DEFAULT_ORACLE_SCHEMA_NAME,
        )
        assert all_tables, f"schema {DEFAULT_ORACLE_SCHEMA_NAME!r} must have at least one object"

        limited = conn.get_tables(
            conn_executor_factory=sync_conn_executor_factory_for_conn,
            schema_name=DEFAULT_ORACLE_SCHEMA_NAME,
            limit=1,
        )
        assert len(limited) == 1
        assert limited[0] in all_tables

        if len(all_tables) > 1:
            with_offset = conn.get_tables(
                conn_executor_factory=sync_conn_executor_factory_for_conn,
                schema_name=DEFAULT_ORACLE_SCHEMA_NAME,
                limit=1,
                offset=1,
            )
            assert len(with_offset) == 1
            assert with_offset[0] != limited[0]

        search_prefix = all_tables[0].table_name[:3]
        matched = conn.get_tables(
            conn_executor_factory=sync_conn_executor_factory_for_conn,
            schema_name=DEFAULT_ORACLE_SCHEMA_NAME,
            search_text=search_prefix,
        )
        assert matched
        assert all(search_prefix.lower() in t.table_name.lower() for t in matched)

        empty = conn.get_tables(
            conn_executor_factory=sync_conn_executor_factory_for_conn,
            schema_name=DEFAULT_ORACLE_SCHEMA_NAME,
            search_text="__no_such_table_xyz__",
        )
        assert len(empty) == 0


class TestSSLOracleConnection(
    BaseSSLOracleTestClass,
    DefaultConnectionTestClass[ConnectionSQLOracle],
):
    do_check_data_export_flag = True

    def check_saved_connection(self, conn: ConnectionSQLOracle, params: dict) -> None:
        assert conn.uuid is not None
        assert conn.data.db_name == params["db_name"]
        assert conn.data.host == params["host"]
        assert conn.data.port == params["port"]
        assert conn.data.username == params["username"]
        assert conn.data.password == params["password"]
        assert conn.data.ssl_enable is params["ssl_enable"]
        assert conn.data.ssl_ca == params["ssl_ca"]

    def check_data_source_templates(
        self,
        conn: ConnectionSQLOracle,
        dsrc_templates: list[DataSourceTemplate],
    ) -> None:
        assert dsrc_templates
        for dsrc_tmpl in dsrc_templates:
            assert dsrc_tmpl.title
            if dsrc_tmpl.parameters.get("schema_name") is not None:
                assert dsrc_tmpl.group == [dsrc_tmpl.parameters["schema_name"]]

        schema_names = {tmpl.parameters.get("schema_name") for tmpl in dsrc_templates}
        assert DEFAULT_ORACLE_SCHEMA_NAME in schema_names

    @pytest.mark.parametrize(
        "ssl_ca_filename",
        [
            "invalid-ca.pem",  # [SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: self-signed certificate in certificate chain
            # TODO: add cases for
            # [SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: IP address mismatch, certificate is not valid for ...
            # [SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: invalid CA certificate
        ],
    )
    def test_bad_ssl_ca(
        self,
        saved_connection: ConnectionSQLOracle,
        sync_conn_executor_factory: Callable[[], OracleDefaultConnExecutor],
        ssl_ca_filename: str,
    ) -> None:
        def sync_conn_executor_factory_for_conn(_connection: ConnectionBase) -> OracleDefaultConnExecutor:
            return sync_conn_executor_factory()

        uri = f"{CoreSSLConnectionSettings.CERT_PROVIDER_URL}/{ssl_ca_filename}"
        try:
            response = requests.get(uri)
            response.raise_for_status()
            assert response.text
        except Exception as e:
            pytest.fail(f"Failed to fetch {uri} from ssl-provider container: {e}")

        ssl_ca = response.text

        saved_connection.data.ssl_ca = ssl_ca
        with pytest.raises(DatabaseOperationalError):
            saved_connection.test(conn_executor_factory=sync_conn_executor_factory_for_conn)
