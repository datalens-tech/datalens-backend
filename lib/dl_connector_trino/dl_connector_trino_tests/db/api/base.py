import pytest

from dl_api_lib_testing.configuration import ApiTestEnvironmentConfiguration
from dl_api_lib_testing.connection_base import ConnectionTestBase
from dl_api_lib_testing.data_api_base import StandardizedDataApiTestBase
from dl_api_lib_testing.dataset_base import DatasetTestBase
from dl_constants.enums import RawSQLLevel
from dl_core_testing.database import DbTable

from dl_connector_trino.core.constants import (
    CONNECTION_TYPE_TRINO,
    SOURCE_TYPE_TRINO_TABLE,
)
from dl_connector_trino_tests.db.config import (
    API_TEST_CONFIG,
    DB_CORE_URL_MEMORY_CATALOG,
    CoreConnectionSettings,
)
from dl_connector_trino_tests.db.core.base import BaseTrinoTestClass


class TrinoConnectionTestBase(BaseTrinoTestClass, ConnectionTestBase):
    conn_type = CONNECTION_TYPE_TRINO
    compeng_enabled = False

    @pytest.fixture(scope="class")
    def db_url(self) -> str:
        return DB_CORE_URL_MEMORY_CATALOG

    @pytest.fixture(scope="class")
    def bi_test_config(self) -> ApiTestEnvironmentConfiguration:
        return API_TEST_CONFIG

    @pytest.fixture(scope="class")
    def connection_params(self) -> dict:
        return dict(
            host=CoreConnectionSettings.HOST,
            port=CoreConnectionSettings.PORT,
            username=CoreConnectionSettings.USERNAME,
            auth_type=CoreConnectionSettings.AUTH_TYPE.name,
            **(dict(raw_sql_level=self.raw_sql_level.value) if self.raw_sql_level is not None else {}),
        )


class TrinoDashSQLConnectionTest(TrinoConnectionTestBase):
    raw_sql_level = RawSQLLevel.dashsql


class TrinoDatasetTestBase(TrinoConnectionTestBase, DatasetTestBase):
    @pytest.fixture(scope="class")
    def dataset_params(self, sample_table: DbTable) -> dict:
        return dict(
            source_type=SOURCE_TYPE_TRINO_TABLE.name,
            parameters=dict(
                db_name=sample_table.db.name,
                schema_name=sample_table.schema,
                table_name=sample_table.name,
            ),
        )


class TrinoDataApiTestBase(TrinoDatasetTestBase, StandardizedDataApiTestBase):
    mutation_caches_enabled = False
