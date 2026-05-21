import pytest

import dl_api_lib_testing
from dl_core_testing.database import DbTable

from dl_connector_postgresql.core.postgresql.constants import (
    SOURCE_TYPE_PG_SUBSELECT,
    SOURCE_TYPE_PG_TABLE,
)
from dl_connector_postgresql.core.postgresql.settings import PostgreSQLConnectorSettings
from dl_connector_postgresql_tests.db.api.base import PostgreSQLDatasetTestBase


class TestDatasourceManualTable(
    dl_api_lib_testing.BaseTestDatasourceManualTable,
    PostgreSQLDatasetTestBase,
):
    source_type = SOURCE_TYPE_PG_TABLE
    conn_settings_cls = PostgreSQLConnectorSettings

    @pytest.fixture(name="parameters")
    def fixture_parameters(self, sample_table: DbTable) -> dict:
        return dict(schema_name=sample_table.schema, table_name=sample_table.name)

    @pytest.fixture(name="missing_table_parameters")
    def fixture_missing_table_parameters(self, sample_table: DbTable) -> dict:
        return dict(schema_name=sample_table.schema, table_name="table_that_does_not_exist")

    @pytest.fixture(name="templated_table_parameters")
    def fixture_templated_table_parameters(self, sample_table: DbTable) -> dict:
        return dict(schema_name=sample_table.schema, table_name="{{table_name}}")


class TestDatasourceManualSubselect(
    dl_api_lib_testing.BaseTestDatasourceManualSubselect,
    PostgreSQLDatasetTestBase,
):
    source_type = SOURCE_TYPE_PG_SUBSELECT
    conn_settings_cls = PostgreSQLConnectorSettings

    @pytest.fixture(name="parameters")
    def fixture_parameters(self, sample_table: DbTable) -> dict:
        return dict(subsql=f'select * from "{sample_table.name}"')


class TestDatasourceManualRawSqlLevelOff(
    dl_api_lib_testing.BaseTestDatasourceManualRawSqlLevelOff,
    PostgreSQLDatasetTestBase,
):
    source_type = SOURCE_TYPE_PG_TABLE
    conn_settings_cls = PostgreSQLConnectorSettings

    @pytest.fixture(name="parameters")
    def fixture_parameters(self, sample_table: DbTable) -> dict:
        return dict(schema_name=sample_table.schema, table_name=sample_table.name)
