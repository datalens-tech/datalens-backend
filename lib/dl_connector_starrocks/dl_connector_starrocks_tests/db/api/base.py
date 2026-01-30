import pytest

from dl_api_lib_testing.configuration import ApiTestEnvironmentConfiguration
from dl_api_lib_testing.connection_base import ConnectionTestBase
from dl_api_lib_testing.data_api_base import StandardizedDataApiTestBase
from dl_api_lib_testing.dataset_base import DatasetTestBase

from dl_connector_starrocks.core.constants import (
    CONNECTION_TYPE_STARROCKS,
    SOURCE_TYPE_STARROCKS_TABLE,
)
from dl_connector_starrocks_tests.db.config import (
    API_TEST_CONFIG,
    CoreConnectionSettings,
)
from dl_connector_starrocks_tests.db.core.base import BaseStarRocksTestClass


class StarRocksConnectionTestBase(BaseStarRocksTestClass, ConnectionTestBase):  # type: ignore  # 2024-01-30 # TODO: fix
    conn_type = CONNECTION_TYPE_STARROCKS
    compeng_enabled = False

    @pytest.fixture(scope="class")
    def bi_test_config(self) -> ApiTestEnvironmentConfiguration:
        return API_TEST_CONFIG

    @pytest.fixture(scope="class")
    def connection_params(self) -> dict:
        return dict(
            db_name=CoreConnectionSettings.DB_NAME,
            host=CoreConnectionSettings.HOST,
            port=CoreConnectionSettings.PORT,
            username=CoreConnectionSettings.USERNAME,
            password=CoreConnectionSettings.PASSWORD,
        )


class StarRocksDatasetTestBase(StarRocksConnectionTestBase, DatasetTestBase):  # type: ignore  # 2024-01-30 # TODO: fix
    @pytest.fixture(scope="class")
    def dataset_params(self, sample_table) -> dict:  # type: ignore  # 2024-01-30 # TODO: fix
        return dict(
            source_type=SOURCE_TYPE_STARROCKS_TABLE.name,
            parameters=dict(
                table_name=sample_table.name,
            ),
        )


class StarRocksDataApiTestBase(StarRocksDatasetTestBase, StandardizedDataApiTestBase):  # type: ignore  # 2024-01-30 # TODO: fix
    mutation_caches_enabled = False
