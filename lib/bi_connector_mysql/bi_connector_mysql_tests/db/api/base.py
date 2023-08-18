import pytest

from bi_constants.enums import RawSQLLevel

from bi_api_lib_testing.configuration import BiApiTestEnvironmentConfiguration

from bi_connector_mysql.core.constants import CONNECTION_TYPE_MYSQL, SOURCE_TYPE_MYSQL_TABLE

from bi_api_lib_testing.connection_base import ConnectionTestBase
from bi_api_lib_testing.dataset_base import DatasetTestBase
from bi_api_lib_testing.data_api_base import StandardizedDataApiTestBase

from bi_connector_mysql_tests.db.config import (
    BI_TEST_CONFIG, CoreConnectionSettings,
)
from bi_connector_mysql_tests.db.core.base import BaseMySQLTestClass


class MySQLConnectionTestBase(BaseMySQLTestClass, ConnectionTestBase):
    conn_type = CONNECTION_TYPE_MYSQL
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
            **(dict(raw_sql_level=self.raw_sql_level.value) if self.raw_sql_level is not None else {}),
        )


class MySQLDashSQLConnectionTest(MySQLConnectionTestBase):
    raw_sql_level = RawSQLLevel.dashsql


class MySQLDatasetTestBase(MySQLConnectionTestBase, DatasetTestBase):
    @pytest.fixture(scope='class')
    def dataset_params(self, sample_table) -> dict:
        return dict(
            is_ref=False,
            source_type=SOURCE_TYPE_MYSQL_TABLE.name,
            parameters=dict(
                db_name=sample_table.db.name,
                table_name=sample_table.name,
            ),
        )


class MySQLDataApiTestBase(MySQLDatasetTestBase, StandardizedDataApiTestBase):
    mutation_caches_on = False
