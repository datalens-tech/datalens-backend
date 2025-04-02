from dl_core.us_connection_base import DataSourceTemplate
from dl_core_testing.testcases.connection import DefaultConnectionTestClass

from dl_connector_ydb.core.ydb.us_connection import YDBConnection
from dl_connector_ydb_tests.db.core.base import (
    BaseSSLYDBTestClass,
    BaseYDBTestClass,
)


class TestYDBConnection(
    BaseYDBTestClass,
    DefaultConnectionTestClass[YDBConnection],
):
    do_check_data_export_flag = True

    def check_saved_connection(self, conn: YDBConnection, params: dict) -> None:
        assert conn.uuid is not None
        assert conn.data.db_name == params["db_name"]
        assert conn.data.host == params["host"]
        assert conn.data.port == params["port"]
        assert conn.data.ssl_enable is False
        assert conn.data.ssl_ca is None

    def check_data_source_templates(
        self,
        conn: YDBConnection,
        dsrc_templates: list[DataSourceTemplate],
    ) -> None:
        # TODO: Implement check after implementing _empty_table
        pass


class TestSSLYDBConnection(
    BaseSSLYDBTestClass,
    DefaultConnectionTestClass[YDBConnection],
):
    def check_saved_connection(self, conn: YDBConnection, params: dict) -> None:
        assert conn.uuid is not None
        assert conn.data.db_name == params["db_name"]
        assert conn.data.host == params["host"]
        assert conn.data.port == params["port"]
        assert conn.data.ssl_enable is params["ssl_enable"]
        assert conn.data.ssl_ca == params["ssl_ca"]

    def check_data_source_templates(
        self,
        conn: YDBConnection,
        dsrc_templates: list[DataSourceTemplate],
    ) -> None:
        # TODO: Implement check after implementing _empty_table
        pass
