import pytest

from bi_api_lib_testing.configuration import BiApiTestEnvironmentConfiguration

from bi_connector_mssql.core.constants import CONNECTION_TYPE_MSSQL, SOURCE_TYPE_MSSQL_TABLE

from bi_api_lib_testing.connection_base import ConnectionTestBase
from bi_api_lib_testing.dataset_base import DatasetTestBase
from bi_api_lib_testing.data_api_base import StandardizedDataApiTestBase

from bi_connector_mssql_tests.db.config import (
    BI_TEST_CONFIG, CoreConnectionSettings,
)
from bi_connector_mssql_tests.db.core.base import BaseMSSQLTestClass


class MSSQLConnectionTestBase(BaseMSSQLTestClass, ConnectionTestBase):
    conn_type = CONNECTION_TYPE_MSSQL
    bi_compeng_pg_on = False

    @pytest.fixture(scope='class')
    def bi_test_config(self) -> BiApiTestEnvironmentConfiguration:
        return BI_TEST_CONFIG

    @pytest.fixture(scope='class')
    def connection_params(self) -> dict:
        return dict(
            db_name=CoreConnectionSettings.DB_NAME,
            host=CoreConnectionSettings.HOST,
            port=CoreConnectionSettings.PORT,
            username=CoreConnectionSettings.USERNAME,
            password=CoreConnectionSettings.PASSWORD,
        )


class MSSQLDatasetTestBase(MSSQLConnectionTestBase, DatasetTestBase):
    @pytest.fixture(scope='class')
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
    mutation_caches_on = False
