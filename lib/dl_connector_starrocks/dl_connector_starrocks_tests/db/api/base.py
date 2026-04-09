import pytest

from dl_api_lib_testing.configuration import ApiTestEnvironmentConfiguration
from dl_api_lib_testing.connection_base import ConnectionTestBase
from dl_api_lib_testing.connector import (
    complex_queries,
    data_api_suites,
)
from dl_api_lib_testing.data_api_base import StandardizedDataApiTestBase
from dl_api_lib_testing.dataset_base import DatasetTestBase
from dl_api_lib_testing.helpers import data_source
from dl_constants.enums import RawSQLLevel
from dl_core_testing.database import DbTable

from dl_connector_starrocks.core.constants import (
    CONNECTION_TYPE_STARROCKS,
    SOURCE_TYPE_STARROCKS_TABLE,
)
from dl_connector_starrocks_tests.db.config import (
    API_TEST_CONFIG,
    CoreConnectionSettings,
)
from dl_connector_starrocks_tests.db.core.base import BaseStarRocksTestClass


class StarRocksConnectionTestBase(BaseStarRocksTestClass, ConnectionTestBase):
    conn_type = CONNECTION_TYPE_STARROCKS
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
            listing_sources=CoreConnectionSettings.LISTING_SOURCES.name,
            **(dict(raw_sql_level=self.raw_sql_level.value) if self.raw_sql_level is not None else {}),
        )


class StarRocksDashSQLConnectionTest(StarRocksConnectionTestBase):
    raw_sql_level = RawSQLLevel.dashsql


class StarRocksDatasetTestBase(StarRocksConnectionTestBase, DatasetTestBase):
    @pytest.fixture(scope="function")
    def source_listing_values(self) -> dict[str, bool | str | None]:
        return {
            "supports_source_search": True,
            "supports_source_pagination": True,
            "supports_db_name_listing": True,
            "db_name_required_for_search": True,
            "db_name_label": "Catalog",
        }

    @pytest.fixture(scope="class")
    def dataset_params(self, sample_table: DbTable) -> dict:
        return dict(
            source_type=SOURCE_TYPE_STARROCKS_TABLE.name,
            parameters=dict(
                db_name=CoreConnectionSettings.CATALOG,
                schema_name=sample_table.db.name,
                table_name=sample_table.name,
            ),
        )


class StarRocksDataApiTestBase(StarRocksDatasetTestBase, StandardizedDataApiTestBase):
    mutation_caches_enabled = False

    @pytest.fixture(autouse=True, scope="class")
    def _starrocks_catalog_patch(self):
        """Remap data_source_settings_from_table for StarRocks catalog.database.table addressing.

        The generic helper maps table.db.name -> db_name, but StarRocks uses
        catalog.database.table: db_name = catalog, schema_name = database.
        """
        orig = data_source.data_source_settings_from_table

        def patched(table: DbTable) -> dict:
            result = orig(table)
            params = result.get("parameters", {})
            if "db_name" in params and "schema_name" not in params:
                params["schema_name"] = params["db_name"]
                params["db_name"] = CoreConnectionSettings.CATALOG
            return result

        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(data_source, "data_source_settings_from_table", patched)
            mp.setattr(complex_queries, "data_source_settings_from_table", patched)
            mp.setattr(data_api_suites, "data_source_settings_from_table", patched)
            yield

    def get_dataset_params(self, dataset_params: dict, db_table: DbTable) -> dict:
        result = super().get_dataset_params(dataset_params, db_table)
        params = result["parameters"]
        # StarRocks uses catalog.database.table addressing:
        # db_name = catalog, schema_name = database
        params["schema_name"] = db_table.db.name
        params["db_name"] = CoreConnectionSettings.CATALOG
        return result
