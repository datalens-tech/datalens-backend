import re
from typing import Callable

import pytest
import requests

from dl_core.exc import DatabaseOperationalError
from dl_core.us_connection_base import (
    ConnectionBase,
    DataSourceTemplate,
)
from dl_core_testing.testcases.connection import DefaultConnectionTestClass

from dl_connector_mysql.core.connection_executors import MySQLConnExecutor
from dl_connector_mysql.core.us_connection import ConnectionMySQL
import dl_connector_mysql_tests.db.config as test_config
from dl_connector_mysql_tests.db.core.base import (
    BaseMySQLTestClass,
    BaseSslMySQLTestClass,
)


class TestMySQLConnection(
    BaseMySQLTestClass,
    DefaultConnectionTestClass[ConnectionMySQL],
):
    do_check_data_export_flag = True

    def check_saved_connection(self, conn: ConnectionMySQL, params: dict) -> None:
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
        conn: ConnectionMySQL,
        dsrc_templates: list[DataSourceTemplate],
    ) -> None:
        assert dsrc_templates
        for dsrc_tmpl in dsrc_templates:
            assert dsrc_tmpl.title


class TestSslMySQLConnection(
    BaseSslMySQLTestClass,
    DefaultConnectionTestClass[ConnectionMySQL],
):
    do_check_data_export_flag = True

    def check_saved_connection(self, conn: ConnectionMySQL, params: dict) -> None:
        assert conn.uuid is not None
        assert conn.data.db_name == params["db_name"]
        assert conn.data.host == params["host"]
        assert conn.data.port == params["port"]
        assert conn.data.username == params["username"]
        assert conn.data.password == params["password"]
        assert conn.data.ssl_enable is True
        assert conn.data.ssl_ca is params["ssl_ca"]

    def check_data_source_templates(
        self,
        conn: ConnectionMySQL,
        dsrc_templates: list[DataSourceTemplate],
    ) -> None:
        assert dsrc_templates
        for dsrc_tmpl in dsrc_templates:
            assert dsrc_tmpl.title

    @pytest.mark.parametrize(
        "ssl_ca_filename, error_message",
        [
            (
                "invalid-ca.pem",
                r"\[SSL: CERTIFICATE_VERIFY_FAILED\] certificate verify failed: self-signed certificate in certificate chain",
            ),
            # TODO: add cases for
            # [SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: IP address mismatch, certificate is not valid for ...
            # [SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: invalid CA certificate
        ],
    )
    def test_bad_ssl_ca(
        self,
        saved_connection: ConnectionMySQL,
        sync_conn_executor_factory: Callable[[], MySQLConnExecutor],
        ssl_ca_filename: str,
        error_message: str,
    ) -> None:
        def sync_conn_executor_factory_for_conn(connection: ConnectionBase) -> MySQLConnExecutor:
            return sync_conn_executor_factory()

        uri = f"{test_config.CoreSslConnectionSettings.CERT_PROVIDER_URL}/{ssl_ca_filename}"
        response = requests.get(uri)
        assert response.status_code == 200, response.text
        ssl_ca = response.text

        saved_connection.data.ssl_ca = ssl_ca
        with pytest.raises(DatabaseOperationalError) as exc_info:
            saved_connection.test(conn_executor_factory=sync_conn_executor_factory_for_conn)

        assert re.search(error_message, exc_info.value.db_message)
