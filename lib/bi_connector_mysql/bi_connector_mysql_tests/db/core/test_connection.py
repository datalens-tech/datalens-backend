from dl_core.us_connection_base import DataSourceTemplate
from dl_core_testing.testcases.connection import DefaultConnectionTestClass

from bi_connector_mysql.core.us_connection import ConnectionMySQL
from bi_connector_mysql_tests.db.core.base import BaseMySQLTestClass


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

    def check_data_source_templates(
        self,
        conn: ConnectionMySQL,
        dsrc_templates: list[DataSourceTemplate],
    ) -> None:
        assert dsrc_templates
        for dsrc_tmpl in dsrc_templates:
            assert dsrc_tmpl.title
