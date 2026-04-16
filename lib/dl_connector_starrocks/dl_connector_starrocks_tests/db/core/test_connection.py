from typing import Callable

import pytest
import requests

from dl_core.connection_executors.sync_base import SyncConnExecutorBase
from dl_core.exc import DatabaseOperationalError
from dl_core.us_connection_base import (
    ConnectionBase,
    DataSourceTemplate,
)
from dl_core_testing.testcases.connection import DefaultConnectionTestClass

from dl_connector_starrocks.core.constants import STARROCKS_SYSTEM_CATALOGS
from dl_connector_starrocks.core.us_connection import ConnectionStarRocks
import dl_connector_starrocks_tests.db.config as test_config
from dl_connector_starrocks_tests.db.config import CoreConnectionSettings
from dl_connector_starrocks_tests.db.core.base import (
    BaseSslStarRocksTestClass,
    BaseStarRocksTestClass,
)


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
        assert conn.data.ssl_enable == False
        assert conn.data.ssl_ca is None

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


class TestStarRocksSslOnUnsupportedVersion(BaseStarRocksTestClass):
    """Verify that ssl_enable=True against StarRocks without SSL support
    fails with a clear error instead of hanging."""

    @pytest.fixture(scope="function")
    def connection_creation_params(self) -> dict:
        return dict(
            host=test_config.CoreConnectionSettings.HOST,
            port=test_config.CoreConnectionSettings.PORT,
            username=test_config.CoreConnectionSettings.USERNAME,
            password=test_config.CoreConnectionSettings.PASSWORD,
            listing_sources=test_config.CoreConnectionSettings.LISTING_SOURCES,
            ssl_enable=True,
        )

    def test_ssl_on_unsupported_version_fails(
        self,
        saved_connection: ConnectionStarRocks,
        sync_conn_executor_factory: Callable[[], SyncConnExecutorBase],
    ) -> None:
        def sync_conn_executor_factory_for_conn(connection: ConnectionBase) -> SyncConnExecutorBase:
            return sync_conn_executor_factory()

        with pytest.raises(DatabaseOperationalError):
            saved_connection.test(conn_executor_factory=sync_conn_executor_factory_for_conn)


class TestSslStarRocksConnection(
    BaseSslStarRocksTestClass,
    DefaultConnectionTestClass[ConnectionStarRocks],
):
    do_check_data_export_flag = True

    def check_saved_connection(self, conn: ConnectionStarRocks, params: dict) -> None:
        assert conn.uuid is not None
        assert conn.data.host == params["host"]
        assert conn.data.port == params["port"]
        assert conn.data.username == params["username"]
        assert conn.data.password == params["password"]
        assert conn.data.ssl_enable == True
        assert conn.data.ssl_ca == params["ssl_ca"]

    def check_data_source_templates(self, conn: ConnectionStarRocks, dsrc_templates: list[DataSourceTemplate]) -> None:
        assert dsrc_templates
        for dsrc_tmpl in dsrc_templates:
            assert dsrc_tmpl.title

    @pytest.mark.parametrize(
        "ssl_ca_filename",
        [
            "invalid-ca.pem",
        ],
    )
    def test_bad_ssl_ca(
        self,
        saved_connection: ConnectionStarRocks,
        sync_conn_executor_factory: Callable[[], SyncConnExecutorBase],
        ssl_ca_filename: str,
    ) -> None:
        def sync_conn_executor_factory_for_conn(connection: ConnectionBase) -> SyncConnExecutorBase:
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
