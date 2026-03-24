import os
from typing import Callable

import clickhouse_sqlalchemy.exceptions
import pytest

from dl_core.us_connection_base import DataSourceTemplate
from dl_core_testing.testcases.connection import DefaultConnectionTestClass

from dl_connector_clickhouse.core.clickhouse.us_connection import ConnectionClickhouse
import dl_connector_clickhouse_tests.db.config as test_config
from dl_connector_clickhouse_tests.db.core.base import (
    BaseClickHouseDefaultUserTestClass,
    BaseClickHouseReadonlyUserTestClass,
    BaseClickHouseTestClass,
    BaseSslClickHouseTestClass,
    BaseSslNoVerifyClickHouseTestClass,
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
        assert conn.data.ssl_ca == params["ssl_ca"]


@pytest.mark.skipif(os.environ.get("WE_ARE_IN_CI"), reason="can't use localhost")
class TestSslNoVerifyClickHouseConnection(
    BaseSslNoVerifyClickHouseTestClass,
    TestClickHouseConnection,
    DefaultConnectionTestClass[ConnectionClickhouse],
):
    def check_saved_connection(self, conn: ConnectionClickhouse, params: dict) -> None:
        assert conn.uuid is not None
        assert conn.data.db_name == params["db_name"]
        assert conn.data.username == params["username"]
        assert conn.data.secure is True
        assert conn.data.ssl_ca is None
        assert conn.data.ssl_ca_verify is False


@pytest.mark.skipif(os.environ.get("WE_ARE_IN_CI"), reason="can't use localhost")
class TestSslNoVerifyIgnoredWhenSettingDisabled(
    BaseSslClickHouseTestClass,
    BaseClickHouseTestClass,
):

    """SSL ClickHouse with ssl_ca_verify=False stored but ALLOW_SSL_CA_VERIFY_OPTION=False.

    Verifies that when the setting is disabled the stored ssl_ca_verify=False is ignored
    and SSL certificate verification remains active.
    """

    @pytest.fixture(scope="function")
    def connection_creation_params(self) -> dict:
        return dict(
            db_name=test_config.CoreSslConnectionSettings.DB_NAME,
            host=test_config.CoreSslConnectionSettings.HOST,
            port=test_config.CoreSslConnectionSettings.PORT,
            username=test_config.CoreSslConnectionSettings.USERNAME,
            password=test_config.CoreSslConnectionSettings.PASSWORD,
            **(dict(raw_sql_level=self.raw_sql_level) if self.raw_sql_level is not None else {}),
            secure=True,
            ssl_ca_verify=False,  # stored, but must be ignored because ALLOW_SSL_CA_VERIFY_OPTION=False
        )

    def test_connection_fails_when_ssl_ca_verify_ignored(
        self,
        saved_connection: ConnectionClickhouse,
        sync_conn_executor_factory: Callable,
    ) -> None:
        def factory(connection):
            return sync_conn_executor_factory()

        with pytest.raises(clickhouse_sqlalchemy.exceptions.DatabaseException):
            saved_connection.test(conn_executor_factory=factory)
