import pytest

from dl_api_lib_testing.configuration import ApiTestEnvironmentConfiguration
from dl_api_lib_testing.connection_base import ConnectionTestBase
from dl_api_lib_testing.data_api_base import StandardizedDataApiTestBase
from dl_api_lib_testing.dataset_base import DatasetTestBase
from dl_constants.enums import RawSQLLevel

from dl_connector_clickhouse.core.clickhouse.constants import SOURCE_TYPE_CH_TABLE
from dl_connector_clickhouse.core.clickhouse_base.constants import CONNECTION_TYPE_CLICKHOUSE
from dl_connector_clickhouse_tests.db.config import (
    API_TEST_CONFIG,
    CoreConnectionSettings,
    CoreReadonlyConnectionSettings,
)
from dl_connector_clickhouse_tests.db.core.base import BaseClickHouseTestClass


class ClickHouseConnectionTestBase(BaseClickHouseTestClass, ConnectionTestBase):
    conn_type = CONNECTION_TYPE_CLICKHOUSE
    compeng_enabled = False

    @pytest.fixture(scope="class")
    def bi_test_config(self) -> ApiTestEnvironmentConfiguration:
        return API_TEST_CONFIG

    @pytest.fixture(scope="class")
    def connection_params(self) -> dict:
        return dict(
            host=CoreConnectionSettings.HOST,
            port=CoreConnectionSettings.PORT,
            username=CoreConnectionSettings.USERNAME,
            password=CoreConnectionSettings.PASSWORD,
            **(dict(raw_sql_level=self.raw_sql_level.value) if self.raw_sql_level is not None else {}),
        )


class ClickHouseConnectionDefaultUserTestBase(ClickHouseConnectionTestBase):
    @pytest.fixture(scope="class")
    def connection_params(self) -> dict:
        return dict(
            db_name=CoreConnectionSettings.DB_NAME,
            host=CoreConnectionSettings.HOST,
            port=CoreConnectionSettings.PORT,
        )


class ClickHouseConnectionReadonlyUserTestBase(ClickHouseConnectionTestBase):
    @pytest.fixture(scope="class")
    def connection_params(self) -> dict:
        return dict(
            db_name=CoreReadonlyConnectionSettings.DB_NAME,
            host=CoreReadonlyConnectionSettings.HOST,
            port=CoreReadonlyConnectionSettings.PORT,
            username=CoreReadonlyConnectionSettings.USERNAME,
            password=CoreReadonlyConnectionSettings.PASSWORD,
            readonly=1,
        )


class ClickHouseDashSQLConnectionTest(ClickHouseConnectionTestBase):
    raw_sql_level = RawSQLLevel.dashsql


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


class ClickHouseDatasetReadonlyUserTestBase(ClickHouseConnectionReadonlyUserTestBase, ClickHouseDatasetTestBase):
    pass


class ClickHouseDataApiTestBase(ClickHouseDatasetTestBase, StandardizedDataApiTestBase):
    mutation_caches_enabled = False


class ClickHouseDataApiReadonlyUserTestBase(ClickHouseDatasetReadonlyUserTestBase, StandardizedDataApiTestBase):
    mutation_caches_enabled = False
