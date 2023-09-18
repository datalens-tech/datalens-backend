import pytest

from dl_api_lib_testing.configuration import BiApiTestEnvironmentConfiguration
from dl_api_lib_testing.connection_base import ConnectionTestBase
from dl_api_lib_testing.data_api_base import StandardizedDataApiTestBase
from dl_api_lib_testing.dataset_base import DatasetTestBase
from dl_constants.enums import RawSQLLevel

from dl_connector_postgresql.core.postgresql.constants import (
    CONNECTION_TYPE_POSTGRES,
    SOURCE_TYPE_PG_TABLE,
)
from dl_connector_postgresql_tests.db.config import (
    BI_TEST_CONFIG,
    CoreConnectionSettings,
)
from dl_connector_postgresql_tests.db.core.base import BasePostgreSQLTestClass


class PostgreSQLConnectionTestBase(BasePostgreSQLTestClass, ConnectionTestBase):
    conn_type = CONNECTION_TYPE_POSTGRES
    bi_compeng_pg_on = False

    @pytest.fixture(scope="class")
    def bi_test_config(self) -> BiApiTestEnvironmentConfiguration:
        return BI_TEST_CONFIG

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


class PostgreSQLDashSQLConnectionTest(PostgreSQLConnectionTestBase):
    raw_sql_level = RawSQLLevel.dashsql


class PostgreSQLDatasetTestBase(PostgreSQLConnectionTestBase, DatasetTestBase):
    @pytest.fixture(scope="class")
    def dataset_params(self, sample_table) -> dict:
        return dict(
            source_type=SOURCE_TYPE_PG_TABLE.name,
            parameters=dict(
                schema_name=sample_table.schema,
                table_name=sample_table.name,
            ),
        )


class PostgreSQLDataApiTestBase(PostgreSQLDatasetTestBase, StandardizedDataApiTestBase):
    mutation_caches_on = False
