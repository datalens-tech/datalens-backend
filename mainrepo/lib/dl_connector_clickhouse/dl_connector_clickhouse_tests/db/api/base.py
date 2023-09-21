import pytest

from dl_api_lib_testing.configuration import ApiTestEnvironmentConfiguration
from dl_api_lib_testing.connection_base import ConnectionTestBase
from dl_api_lib_testing.data_api_base import StandardizedDataApiTestBase
from dl_api_lib_testing.dataset_base import DatasetTestBase
from dl_connector_clickhouse.core.clickhouse.constants import SOURCE_TYPE_CH_TABLE
from dl_connector_clickhouse.core.clickhouse_base.constants import CONNECTION_TYPE_CLICKHOUSE
from dl_connector_clickhouse_tests.db.config import (
    API_TEST_CONFIG,
    CoreConnectionSettings,
)
from dl_connector_clickhouse_tests.db.core.base import BaseClickHouseTestClass


class ClickHouseConnectionTestBase(BaseClickHouseTestClass, ConnectionTestBase):
    conn_type = CONNECTION_TYPE_CLICKHOUSE
    bi_compeng_pg_on = False

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


class ClickHouseDatasetTestBase(ClickHouseConnectionTestBase, DatasetTestBase):
    @pytest.fixture(scope="class")
    def dataset_params(self, sample_table) -> dict:
        return dict(
            source_type=SOURCE_TYPE_CH_TABLE.name,
            parameters=dict(
                db_name=sample_table.db.name,
                table_name=sample_table.name,
            ),
        )


class ClickHouseDataApiTestBase(ClickHouseDatasetTestBase, StandardizedDataApiTestBase):
    mutation_caches_on = False
