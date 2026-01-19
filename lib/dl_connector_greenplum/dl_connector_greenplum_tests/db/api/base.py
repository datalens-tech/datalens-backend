from typing import ClassVar

import pytest

from dl_api_lib_testing.configuration import ApiTestEnvironmentConfiguration
from dl_api_lib_testing.connection_base import ConnectionTestBase
from dl_api_lib_testing.data_api_base import StandardizedDataApiTestBase
from dl_api_lib_testing.dataset_base import DatasetTestBase
from dl_constants.enums import RawSQLLevel
from dl_core_testing.database import DbTable
from dl_core_testing.testcases.service_base import ServiceFixtureTextClass

from dl_connector_greenplum.core.constants import (
    CONNECTION_TYPE_GREENPLUM,
    SOURCE_TYPE_GP_TABLE,
)
from dl_connector_greenplum_tests.db.config import (
    API_TEST_CONFIG,
    CONNECTION_PARAMS_BY_VERSION,
    CORE_TEST_CONFIG,
    DB_URLS,
    GreenplumVersion,
)


class GreenplumConnectionTestBase(ConnectionTestBase, ServiceFixtureTextClass):
    conn_type = CONNECTION_TYPE_GREENPLUM
    core_test_config = CORE_TEST_CONFIG
    compeng_enabled = False
    raw_sql_level: ClassVar[RawSQLLevel] = RawSQLLevel.dashsql

    @pytest.fixture(scope="class")
    def bi_test_config(self) -> ApiTestEnvironmentConfiguration:
        return API_TEST_CONFIG


class GP6ConnectionTestBase(GreenplumConnectionTestBase):
    @pytest.fixture(scope="class")
    def db_url(self) -> str:
        return DB_URLS[GreenplumVersion.GP6]

    @pytest.fixture(scope="class")
    def connection_params(self) -> dict:
        result = CONNECTION_PARAMS_BY_VERSION[GreenplumVersion.GP6].copy()
        if self.raw_sql_level is not None:
            result["raw_sql_level"] = self.raw_sql_level.value
        return result


class GP7ConnectionTestBase(GreenplumConnectionTestBase):
    @pytest.fixture(scope="class")
    def db_url(self) -> str:
        return DB_URLS[GreenplumVersion.GP7]

    @pytest.fixture(scope="class")
    def connection_params(self) -> dict:
        result = CONNECTION_PARAMS_BY_VERSION[GreenplumVersion.GP7].copy()
        if self.raw_sql_level is not None:
            result["raw_sql_level"] = self.raw_sql_level.value
        return result


class GreenplumDashSQLConnectionTestMixin:
    raw_sql_level: ClassVar[RawSQLLevel] = RawSQLLevel.dashsql


class GP6DashSQLConnectionTest(GreenplumDashSQLConnectionTestMixin, GP6ConnectionTestBase):
    pass


class GP7DashSQLConnectionTest(GreenplumDashSQLConnectionTestMixin, GP7ConnectionTestBase):
    pass


class GreenplumDatasetTestBaseMixin(DatasetTestBase):
    @pytest.fixture(scope="class")
    def dataset_params(self, sample_table: DbTable) -> dict:
        return dict(
            source_type=SOURCE_TYPE_GP_TABLE.name,
            parameters=dict(
                db_name=sample_table.db.name,
                table_name=sample_table.name,
            ),
        )


class GP6DatasetTestBase(GP6ConnectionTestBase, GreenplumDatasetTestBaseMixin):
    pass


class GP7DatasetTestBase(GP7ConnectionTestBase, GreenplumDatasetTestBaseMixin):
    pass


class GreenplumDataApiTestBaseMixin(StandardizedDataApiTestBase):
    mutation_caches_enabled = False


class GP6DataApiTestBase(GP6DatasetTestBase, GreenplumDataApiTestBaseMixin):
    pass


class GP7DataApiTestBase(GP7DatasetTestBase, GreenplumDataApiTestBaseMixin):
    pass
