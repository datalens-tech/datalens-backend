from typing import Any

import pytest

import dl_api_lib_testing
from dl_core_testing.database import DbTable

from dl_connector_starrocks.core.constants import (
    SOURCE_TYPE_STARROCKS_SUBSELECT,
    SOURCE_TYPE_STARROCKS_TABLE,
)
from dl_connector_starrocks.core.settings import StarRocksConnectorSettings
from dl_connector_starrocks_tests.db.api.base import StarRocksDatasetTestBase
from dl_connector_starrocks_tests.db.config import CoreConnectionSettings


class TestDatasourceManualTable(
    dl_api_lib_testing.BaseTestDatasourceManualTable,
    StarRocksDatasetTestBase,
):
    source_type = SOURCE_TYPE_STARROCKS_TABLE
    conn_settings_cls = StarRocksConnectorSettings

    @pytest.fixture(name="parameters")
    def fixture_parameters(self, sample_table: DbTable) -> dict:
        return {
            "db_name": CoreConnectionSettings.CATALOG,
            "schema_name": sample_table.db.name,
            "table_name": sample_table.name,
        }

    @pytest.fixture(name="missing_table_parameters")
    def fixture_missing_table_parameters(self, sample_table: DbTable) -> dict:
        return {
            "db_name": CoreConnectionSettings.CATALOG,
            "schema_name": sample_table.db.name,
            "table_name": "table_that_does_not_exist",
        }

    @pytest.fixture(name="templated_table_parameters")
    def fixture_templated_table_parameters(self, sample_table: DbTable) -> dict:
        return {
            "db_name": CoreConnectionSettings.CATALOG,
            "schema_name": sample_table.db.name,
            "table_name": "{{table_name}}",
        }


class TestDatasourceManualSubselect(
    dl_api_lib_testing.BaseTestDatasourceManualSubselect,
    StarRocksDatasetTestBase,
):
    source_type = SOURCE_TYPE_STARROCKS_SUBSELECT
    conn_settings_cls = StarRocksConnectorSettings

    @pytest.fixture(name="parameters")
    def fixture_parameters(self, sample_table: DbTable) -> dict:
        return {"subsql": f"select * from {CoreConnectionSettings.CATALOG}.{sample_table.db.name}.{sample_table.name}"}


class TestDatasourceManualRawSqlLevelOff(
    dl_api_lib_testing.BaseTestDatasourceManualRawSqlLevelOff,
    StarRocksDatasetTestBase,
):
    source_type = SOURCE_TYPE_STARROCKS_TABLE
    conn_settings_cls = StarRocksConnectorSettings

    @pytest.fixture(name="parameters")
    def fixture_parameters(self, sample_table: DbTable) -> dict:
        return {
            "db_name": CoreConnectionSettings.CATALOG,
            "schema_name": sample_table.db.name,
            "table_name": sample_table.name,
        }

    @pytest.mark.skip(reason="StarRocks validator accepts manual=True table at raw_sql_level=off")
    def test_manual_true_table_rejected_at_raw_sql_level_off(self, *args: Any, **kwargs: Any) -> None: ...
