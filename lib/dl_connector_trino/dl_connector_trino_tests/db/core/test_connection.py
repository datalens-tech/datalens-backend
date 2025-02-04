from dl_core.us_connection_base import DataSourceTemplate
from dl_core_testing.testcases.connection import DefaultConnectionTestClass

from dl_connector_trino.core.constants import TrinoAuthType
from dl_connector_trino.core.us_connection import ConnectionTrino
from dl_connector_trino_tests.db.core.base import (  # BaseTrinoJwtTestClass,; BaseTrinoPasswordTestClass,
    BaseTrinoTestClass,
)


class TestTrinoConnection(
    BaseTrinoTestClass,
    DefaultConnectionTestClass[ConnectionTrino],
):
    do_check_data_export_flag = True

    def check_saved_connection(self, conn: ConnectionTrino, params: dict) -> None:
        assert conn.uuid is not None
        assert conn.data.host == params["host"]
        assert conn.data.port == params["port"]
        assert conn.data.username == params["username"]
        assert conn.data.auth_type == TrinoAuthType.NONE

    def check_data_source_templates(
        self,
        conn: ConnectionTrino,
        dsrc_templates: list[DataSourceTemplate],
    ) -> None:
        assert dsrc_templates
        for dsrc_tmpl in dsrc_templates:
            assert dsrc_tmpl.title

        tmpl_db_names = {dsrc_tmpl.group[0] for dsrc_tmpl in dsrc_templates}
        assert "system" not in tmpl_db_names


# class TestTrinoPasswordConnection(BaseTrinoPasswordTestClass, TestTrinoConnection):
#     def check_saved_connection(self, conn: ConnectionTrino, params: dict) -> None:
#         assert conn.uuid is not None
#         assert conn.data.host == params["host"]
#         assert conn.data.port == params["port"]
#         assert conn.data.username == params["username"]
#         assert conn.data.password == params["password"]
#         assert conn.data.auth_type == TrinoAuthType.PASSWORD
#         assert conn.data.ssl_ca == params["ssl_ca"]


# class TestTrinoJwtConnection(BaseTrinoJwtTestClass, TestTrinoConnection):
#     def check_saved_connection(self, conn: ConnectionTrino, params: dict) -> None:
#         assert conn.uuid is not None
#         assert conn.data.host == params["host"]
#         assert conn.data.port == params["port"]
#         assert conn.data.username == params["username"]
#         assert conn.data.jwt == params["jwt"]
#         assert conn.data.auth_type == TrinoAuthType.JWT
#         assert conn.data.ssl_ca == params["ssl_ca"]
