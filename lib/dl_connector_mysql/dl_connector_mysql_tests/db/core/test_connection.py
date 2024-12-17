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
        assert conn.data.ssl_enable == False
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
        assert conn.data.ssl_enable == True
        assert conn.data.ssl_ca == params["ssl_ca"]

    def check_data_source_templates(
        self,
        conn: ConnectionMySQL,
        dsrc_templates: list[DataSourceTemplate],
    ) -> None:
        assert dsrc_templates
        for dsrc_tmpl in dsrc_templates:
            assert dsrc_tmpl.title

    @pytest.mark.parametrize(
        "ssl_ca_filename",
        [
            "invalid-ca.pem", # [SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: self-signed certificate in certificate chain
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
    ) -> None:
        def sync_conn_executor_factory_for_conn(connection: ConnectionBase) -> MySQLConnExecutor:
            return sync_conn_executor_factory()

        uri = f"{test_config.CoreSslConnectionSettings.CERT_PROVIDER_URL}/{ssl_ca_filename}"
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
