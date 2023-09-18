from typing import Generator

import pytest

from bi_connector_bundle_ch_filtered_ya_cloud.ch_geo_filtered.core.constants import SOURCE_TYPE_CH_GEO_FILTERED_TABLE
from bi_connector_bundle_ch_filtered_ya_cloud.ch_geo_filtered.core.testing.connection import (
    make_saved_ch_geo_filtered_connection,
)

from dl_api_lib_testing.configuration import BiApiTestEnvironmentConfiguration
from dl_api_lib_testing.connection_base import ConnectionTestBase
from dl_api_lib_testing.dataset_base import DatasetTestBase
from dl_api_lib_testing.data_api_base import StandardizedDataApiTestBase

from dl_core.exc import USReqException
from dl_core.us_manager.us_manager_sync import SyncUSManager
from dl_core_testing.database import DbTable

from dl_connector_clickhouse.db_testing.engine_wrapper import ClickhouseDbEngineConfig
from bi_connector_bundle_ch_filtered_ya_cloud_tests.db.config import BI_TEST_CONFIG
from bi_connector_bundle_ch_filtered_ya_cloud_tests.db.ch_geo_filtered.core.base import BaseCHGeoFilteredClass
from bi_connector_bundle_ch_filtered_ya_cloud_tests.db.ch_geo_filtered.config import (
    CONNECTION_PARAMS, DOWNLOADABLE_CONNECTION_PARAMS
)


class CHGeoFilteredConnectionTestBase(BaseCHGeoFilteredClass, ConnectionTestBase):
    bi_compeng_pg_on = False

    @pytest.fixture(scope='class')
    def engine_config(self, db_url: str, engine_params: dict) -> ClickhouseDbEngineConfig:
        return ClickhouseDbEngineConfig(url=db_url, engine_params=engine_params)

    @pytest.fixture(scope='class')
    def bi_test_config(self) -> BiApiTestEnvironmentConfiguration:
        return BI_TEST_CONFIG

    @pytest.fixture(scope='class')
    def connection_params(self, sample_table: DbTable) -> dict:
        return CONNECTION_PARAMS | dict(
            allowed_tables=[sample_table.name],
            db_name=sample_table.db.name,
        )

    @pytest.fixture(scope='function')
    def saved_connection_id(
            self, conn_default_sync_us_manager: SyncUSManager, connection_params: dict,
    ) -> Generator[str, None, None]:
        usm = conn_default_sync_us_manager
        conn = make_saved_ch_geo_filtered_connection(
            sync_usm=usm,
            **connection_params
        )

        usm.save(conn)
        yield conn.uuid
        try:
            usm.delete(conn)
        except USReqException:  # in case connection was deleted in tests
            pass


class CHGeoFilteredDownloadableConnectionTestBase(CHGeoFilteredConnectionTestBase):
    @pytest.fixture(scope='class')
    def connection_params(self, sample_table: DbTable) -> dict:
        return DOWNLOADABLE_CONNECTION_PARAMS | dict(
            allowed_tables=[sample_table.name],
            db_name=sample_table.db.name,
        )


class CHGeoFilteredDatasetTestBase(CHGeoFilteredConnectionTestBase, DatasetTestBase):
    @pytest.fixture(scope='class')
    def dataset_params(self, sample_table: DbTable) -> dict:
        return dict(
            source_type=SOURCE_TYPE_CH_GEO_FILTERED_TABLE.name,
            parameters=dict(
                db_name=sample_table.db.name,
                table_name=sample_table.name,
            ),
        )


class CHGeoFilteredDownloadableDatasetTestBase(CHGeoFilteredDownloadableConnectionTestBase, DatasetTestBase):
    @pytest.fixture(scope='class')
    def dataset_params(self, sample_table: DbTable) -> dict:
        return dict(
            source_type=SOURCE_TYPE_CH_GEO_FILTERED_TABLE.name,
            parameters=dict(
                db_name=sample_table.db.name,
                table_name=sample_table.name,
            ),
        )


class CHGeoFilteredDataApiTestBase(CHGeoFilteredDatasetTestBase, StandardizedDataApiTestBase):
    mutation_caches_on = False


class CHGeoFilteredDownloadableDataApiTestBase(CHGeoFilteredDownloadableDatasetTestBase, StandardizedDataApiTestBase):
    mutation_caches_on = False
