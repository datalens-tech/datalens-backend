import pytest

from dl_api_lib_testing.configuration import ApiTestEnvironmentConfiguration
from dl_api_lib_testing.connection_base import ConnectionTestBase
from dl_api_lib_testing.data_api_base import StandardizedDataApiTestBase
from dl_api_lib_testing.dataset_base import DatasetTestBase
from dl_connector_greenplum.core.constants import (
    CONNECTION_TYPE_GREENPLUM,
    SOURCE_TYPE_GP_TABLE,
)
from dl_connector_greenplum_tests.db.config import (
    API_TEST_CONFIG,
    CONNECTION_PARAMS,
    CORE_TEST_CONFIG,
    DB_CORE_URL,
)
from dl_constants.enums import RawSQLLevel
from dl_core_testing.database import DbTable
from dl_core_testing.testcases.service_base import ServiceFixtureTextClass


class GreenplumConnectionTestBase(ConnectionTestBase, ServiceFixtureTextClass):
    conn_type = CONNECTION_TYPE_GREENPLUM
    core_test_config = CORE_TEST_CONFIG
    bi_compeng_pg_on = False

    @pytest.fixture(scope="class")
    def db_url(self) -> str:
        return DB_CORE_URL

    @pytest.fixture(scope="class")
    def bi_test_config(self) -> ApiTestEnvironmentConfiguration:
        return API_TEST_CONFIG

    @pytest.fixture(scope="class")
    def connection_params(self) -> dict:
        return CONNECTION_PARAMS


class GreenplumDashSQLConnectionTest(GreenplumConnectionTestBase):
    @pytest.fixture(scope="class")
    def connection_params(self) -> dict:
        return CONNECTION_PARAMS | dict(raw_sql_level=RawSQLLevel.dashsql.value)


class GreenplumDatasetTestBase(GreenplumConnectionTestBase, DatasetTestBase):
    @pytest.fixture(scope="class")
    def dataset_params(self, sample_table: DbTable) -> dict:
        return dict(
            source_type=SOURCE_TYPE_GP_TABLE.name,
            parameters=dict(
                db_name=sample_table.db.name,
                table_name=sample_table.name,
            ),
        )


class GreenplumDataApiTestBase(GreenplumDatasetTestBase, StandardizedDataApiTestBase):
    mutation_caches_on = False
