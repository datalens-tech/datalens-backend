import pytest

import dl_api_lib_testing
from dl_core_testing.database import DbTable

from dl_connector_clickhouse.core.clickhouse.constants import (
    SOURCE_TYPE_CH_SUBSELECT,
    SOURCE_TYPE_CH_TABLE,
)
from dl_connector_clickhouse.core.clickhouse.settings import ClickHouseConnectorSettings
from dl_connector_clickhouse_tests.db.api.base import ClickHouseDatasetTestBase


class TestDatasourceManualTable(
    dl_api_lib_testing.BaseTestDatasourceManualTable,
    ClickHouseDatasetTestBase,
):
    source_type = SOURCE_TYPE_CH_TABLE
    conn_settings_cls = ClickHouseConnectorSettings

    @pytest.fixture(name="parameters")
    def fixture_parameters(self, sample_table: DbTable) -> dict:
        return {"db_name": sample_table.db.name, "table_name": sample_table.name}

    @pytest.fixture(name="missing_table_parameters")
    def fixture_missing_table_parameters(self, sample_table: DbTable) -> dict:
        return {"db_name": sample_table.db.name, "table_name": "table_that_does_not_exist"}

    @pytest.fixture(name="templated_table_parameters")
    def fixture_templated_table_parameters(self, sample_table: DbTable) -> dict:
        return {"db_name": sample_table.db.name, "table_name": "{{table_name}}"}


class TestDatasourceManualSubselect(
    dl_api_lib_testing.BaseTestDatasourceManualSubselect,
    ClickHouseDatasetTestBase,
):
    source_type = SOURCE_TYPE_CH_SUBSELECT
    conn_settings_cls = ClickHouseConnectorSettings

    @pytest.fixture(name="parameters")
    def fixture_parameters(self, sample_table: DbTable) -> dict:
        return {"subsql": f"select * from {sample_table.db.name}.{sample_table.name}"}


class TestDatasourceManualRawSqlLevelOff(
    dl_api_lib_testing.BaseTestDatasourceManualRawSqlLevelOff,
    ClickHouseDatasetTestBase,
):
    source_type = SOURCE_TYPE_CH_TABLE
    conn_settings_cls = ClickHouseConnectorSettings

    @pytest.fixture(name="parameters")
    def fixture_parameters(self, sample_table: DbTable) -> dict:
        return {"db_name": sample_table.db.name, "table_name": sample_table.name}
