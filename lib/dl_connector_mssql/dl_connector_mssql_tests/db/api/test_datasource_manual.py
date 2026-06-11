import pytest

import dl_api_lib_testing
from dl_core_testing.database import DbTable

from dl_connector_mssql.core.constants import (
    SOURCE_TYPE_MSSQL_SUBSELECT,
    SOURCE_TYPE_MSSQL_TABLE,
)
from dl_connector_mssql.core.settings import MSSQLConnectorSettings
from dl_connector_mssql_tests.db.api.base import MSSQLDatasetTestBase


class TestDatasourceManualTable(
    dl_api_lib_testing.BaseTestDatasourceManualTable,
    MSSQLDatasetTestBase,
):
    source_type = SOURCE_TYPE_MSSQL_TABLE
    conn_settings_cls = MSSQLConnectorSettings

    @pytest.fixture(name="parameters")
    def fixture_parameters(self, sample_table: DbTable) -> dict:
        return {
            "db_name": sample_table.db.name,
            "schema_name": sample_table.schema,
            "table_name": sample_table.name,
        }

    @pytest.fixture(name="missing_table_parameters")
    def fixture_missing_table_parameters(self, sample_table: DbTable) -> dict:
        return {
            "db_name": sample_table.db.name,
            "schema_name": sample_table.schema,
            "table_name": "table_that_does_not_exist",
        }

    @pytest.fixture(name="templated_table_parameters")
    def fixture_templated_table_parameters(self, sample_table: DbTable) -> dict:
        return {
            "db_name": sample_table.db.name,
            "schema_name": sample_table.schema,
            "table_name": "{{table_name}}",
        }


class TestDatasourceManualSubselect(
    dl_api_lib_testing.BaseTestDatasourceManualSubselect,
    MSSQLDatasetTestBase,
):
    source_type = SOURCE_TYPE_MSSQL_SUBSELECT
    conn_settings_cls = MSSQLConnectorSettings

    @pytest.fixture(name="parameters")
    def fixture_parameters(self, sample_table: DbTable) -> dict:
        return {"subsql": f'select * from "{sample_table.name}"'}


class TestDatasourceManualRawSqlLevelOff(
    dl_api_lib_testing.BaseTestDatasourceManualRawSqlLevelOff,
    MSSQLDatasetTestBase,
):
    source_type = SOURCE_TYPE_MSSQL_TABLE
    conn_settings_cls = MSSQLConnectorSettings

    @pytest.fixture(name="parameters")
    def fixture_parameters(self, sample_table: DbTable) -> dict:
        return {
            "db_name": sample_table.db.name,
            "schema_name": sample_table.schema,
            "table_name": sample_table.name,
        }
