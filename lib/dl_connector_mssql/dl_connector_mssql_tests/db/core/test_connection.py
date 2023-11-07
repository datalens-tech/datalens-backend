from dl_core.us_connection_base import DataSourceTemplate
from dl_core_testing.testcases.connection import DefaultConnectionTestClass

from dl_connector_mssql.core.us_connection import ConnectionMSSQL
from dl_connector_mssql_tests.db.core.base import BaseMSSQLTestClass


class TestMSSQLConnection(
    BaseMSSQLTestClass,
    DefaultConnectionTestClass[ConnectionMSSQL],
):
    do_check_data_export_flag = True

    def check_saved_connection(self, conn: ConnectionMSSQL, params: dict) -> None:
        assert conn.uuid is not None
        assert conn.data.db_name == params["db_name"]
        assert conn.data.host == params["host"]
        assert conn.data.port == params["port"]
        assert conn.data.username == params["username"]
        assert conn.data.password == params["password"]

    def check_data_source_templates(
        self,
        conn: ConnectionMSSQL,
        dsrc_templates: list[DataSourceTemplate],
    ) -> None:
        assert dsrc_templates
        for dsrc_tmpl in dsrc_templates:
            assert dsrc_tmpl.title
            if dsrc_tmpl.parameters.get("schema_name") is not None:
                assert dsrc_tmpl.group == [dsrc_tmpl.parameters["schema_name"]]

        schema_names = {tmpl.parameters.get("schema_name") for tmpl in dsrc_templates}
        assert "dbo" in schema_names
