import pytest

from dl_api_lib_testing.configuration import ApiTestEnvironmentConfiguration
from dl_api_lib_testing.connection_base import ConnectionTestBase
from dl_api_lib_testing.data_api_base import StandardizedDataApiTestBase
from dl_api_lib_testing.dataset_base import DatasetTestBase
from dl_constants.enums import RawSQLLevel

from dl_connector_mssql.core.constants import (
    CONNECTION_TYPE_MSSQL,
    SOURCE_TYPE_MSSQL_TABLE,
)
from dl_connector_mssql_tests.db.config import (
    API_TEST_CONFIG,
    CoreConnectionSettings,
)
from dl_connector_mssql_tests.db.core.base import BaseMSSQLTestClass


class MSSQLConnectionTestBase(BaseMSSQLTestClass, ConnectionTestBase):
    conn_type = CONNECTION_TYPE_MSSQL
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
            **(dict(raw_sql_level=self.raw_sql_level.value) if self.raw_sql_level is not None else {}),
        )


class MSSQLDashSQLConnectionTest(MSSQLConnectionTestBase):
    raw_sql_level = RawSQLLevel.dashsql


class MSSQLDatasetTestBase(MSSQLConnectionTestBase, DatasetTestBase):
    @pytest.fixture(scope="class")
    def dataset_params(self, sample_table) -> dict:
        return dict(
            source_type=SOURCE_TYPE_MSSQL_TABLE.name,
            parameters=dict(
                db_name=sample_table.db.name,
                schema_name=sample_table.schema,
                table_name=sample_table.name,
            ),
        )


class MSSQLDataApiTestBase(MSSQLDatasetTestBase, StandardizedDataApiTestBase):
    mutation_caches_enabled = False
