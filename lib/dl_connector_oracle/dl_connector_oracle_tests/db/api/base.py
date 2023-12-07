import pytest

from dl_api_lib_testing.configuration import ApiTestEnvironmentConfiguration
from dl_api_lib_testing.connection_base import ConnectionTestBase
from dl_api_lib_testing.data_api_base import StandardizedDataApiTestBase
from dl_api_lib_testing.dataset_base import DatasetTestBase
from dl_constants.enums import RawSQLLevel

from dl_connector_oracle.core.constants import (
    CONNECTION_TYPE_ORACLE,
    SOURCE_TYPE_ORACLE_TABLE,
    OracleDbNameType,
)
from dl_connector_oracle_tests.db.config import (
    API_TEST_CONFIG,
    CoreConnectionSettings,
)
from dl_connector_oracle_tests.db.core.base import BaseOracleTestClass


class OracleConnectionTestBase(BaseOracleTestClass, ConnectionTestBase):
    conn_type = CONNECTION_TYPE_ORACLE
    compeng_enabled = False

    @pytest.fixture(scope="class")
    def bi_test_config(self) -> ApiTestEnvironmentConfiguration:
        return API_TEST_CONFIG

    @pytest.fixture(scope="class")
    def connection_params(self) -> dict:
        return dict(
            db_connect_method=OracleDbNameType.service_name.name,
            db_name=CoreConnectionSettings.DB_NAME,
            host=CoreConnectionSettings.HOST,
            port=CoreConnectionSettings.PORT,
            username=CoreConnectionSettings.USERNAME,
            password=CoreConnectionSettings.PASSWORD,
            **(dict(raw_sql_level=self.raw_sql_level.value) if self.raw_sql_level is not None else {}),
        )


class OracleDashSQLConnectionTest(OracleConnectionTestBase):
    raw_sql_level = RawSQLLevel.dashsql


class OracleDatasetTestBase(OracleConnectionTestBase, DatasetTestBase):
    @pytest.fixture(scope="class")
    def dataset_params(self, sample_table) -> dict:
        return dict(
            source_type=SOURCE_TYPE_ORACLE_TABLE.name,
            parameters=dict(
                db_name=sample_table.db.name,
                schema_name=sample_table.schema,
                table_name=sample_table.name,
            ),
        )


class OracleDataApiTestBase(OracleDatasetTestBase, StandardizedDataApiTestBase):
    mutation_caches_enabled = False
