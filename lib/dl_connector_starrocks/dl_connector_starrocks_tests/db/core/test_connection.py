from typing import Callable

from dl_core.connection_executors.sync_base import SyncConnExecutorBase
from dl_core.us_connection_base import (
    ConnectionBase,
    DataSourceTemplate,
)
from dl_core_testing.testcases.connection import DefaultConnectionTestClass

from dl_connector_starrocks.core.constants import STARROCKS_SYSTEM_CATALOGS
from dl_connector_starrocks.core.us_connection import ConnectionStarRocks
from dl_connector_starrocks_tests.db.config import CoreConnectionSettings
from dl_connector_starrocks_tests.db.core.base import BaseStarRocksTestClass


class TestStarRocksConnection(
    BaseStarRocksTestClass,
    DefaultConnectionTestClass[ConnectionStarRocks],
):
    do_check_data_export_flag = True

    def check_saved_connection(self, conn: ConnectionStarRocks, params: dict) -> None:
        assert conn.uuid is not None
        assert conn.data.host == params["host"]
        assert conn.data.port == params["port"]
        assert conn.data.username == params["username"]
        assert conn.data.password == params["password"]
        assert conn.data.listing_sources == params["listing_sources"]

    def check_data_source_templates(self, conn: ConnectionStarRocks, dsrc_templates: list[DataSourceTemplate]) -> None:
        assert dsrc_templates
        for dsrc_tmpl in dsrc_templates:
            assert dsrc_tmpl.title

    def test_get_db_names_and_tables(
        self, saved_connection: ConnectionStarRocks, sync_conn_executor_factory: Callable[[], SyncConnExecutorBase]
    ) -> None:
        conn = saved_connection

        def sync_conn_executor_factory_for_conn(connection: ConnectionBase) -> SyncConnExecutorBase:
            return sync_conn_executor_factory()

        db_names = conn.get_db_names(sync_conn_executor_factory_for_conn)
        assert db_names
        assert len(set(db_names) & set(STARROCKS_SYSTEM_CATALOGS)) == 0

        tables = conn.get_tables(sync_conn_executor_factory_for_conn, db_name=CoreConnectionSettings.CATALOG)
        assert tables

    def test_get_parameter_combinations(
        self, saved_connection: ConnectionStarRocks, sync_conn_executor_factory: Callable[[], SyncConnExecutorBase]
    ) -> None:
        conn = saved_connection
        catalog = CoreConnectionSettings.CATALOG

        def sync_conn_executor_factory_for_conn(connection: ConnectionBase) -> SyncConnExecutorBase:
            return sync_conn_executor_factory()

        # Test with specific catalog
        param_combinations = conn.get_parameter_combinations(sync_conn_executor_factory_for_conn, db_name=catalog)
        assert param_combinations

        # All results should be from the specified catalog
        for combination in param_combinations:
            assert combination["db_name"] == catalog
            assert combination["schema_name"]
            assert combination["table_name"]
