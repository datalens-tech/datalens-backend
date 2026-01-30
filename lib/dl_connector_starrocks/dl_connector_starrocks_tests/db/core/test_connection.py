from dl_core.us_connection_base import DataSourceTemplate
from dl_core_testing.testcases.connection import DefaultConnectionTestClass

from dl_connector_starrocks.core.us_connection import ConnectionStarRocks
from dl_connector_starrocks_tests.db.core.base import BaseStarRocksTestClass


class TestStarRocksConnection(
    BaseStarRocksTestClass,
    DefaultConnectionTestClass[ConnectionStarRocks],
):
    do_check_data_export_flag = True

    def check_saved_connection(self, conn: ConnectionStarRocks, params: dict) -> None:  # type: ignore  # 2024-01-30 # TODO: fix
        assert conn.uuid is not None
        assert conn.data.db_name == params["db_name"]
        assert conn.data.host == params["host"]
        assert conn.data.port == params["port"]
        assert conn.data.username == params["username"]
        assert conn.data.password == params["password"]

    def check_data_source_templates(self, conn: ConnectionStarRocks, dsrc_templates: list[DataSourceTemplate]) -> None:  # type: ignore  # 2024-01-30 # TODO: fix
        assert dsrc_templates
        for dsrc_tmpl in dsrc_templates:
            assert dsrc_tmpl.title

    def test_get_tables(self, saved_connection, sync_conn_executor_factory) -> None:  # type: ignore  # 2024-01-30 # TODO: fix
        conn = saved_connection
        tables = conn.get_tables(lambda c: sync_conn_executor_factory())
        assert tables

    def test_get_parameter_combinations(self, saved_connection, sync_conn_executor_factory) -> None:  # type: ignore  # 2024-01-30 # TODO: fix
        conn = saved_connection
        param_combinations = conn.get_parameter_combinations(lambda c: sync_conn_executor_factory())
        assert param_combinations
