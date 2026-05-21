import pytest

import dl_api_lib_testing
from dl_core_testing.database import DbTable

from dl_connector_oracle.core.constants import (
    SOURCE_TYPE_ORACLE_SUBSELECT,
    SOURCE_TYPE_ORACLE_TABLE,
)
from dl_connector_oracle.core.settings import OracleConnectorSettings
from dl_connector_oracle_tests.db.api.base import OracleDatasetTestBase


class TestDatasourceManualTable(
    dl_api_lib_testing.BaseTestDatasourceManualTable,
    OracleDatasetTestBase,
):
    source_type = SOURCE_TYPE_ORACLE_TABLE
    conn_settings_cls = OracleConnectorSettings

    @pytest.fixture(name="parameters")
    def fixture_parameters(self, sample_table: DbTable) -> dict:
        return dict(
            db_name=sample_table.db.name,
            schema_name=sample_table.schema,
            table_name=sample_table.name,
        )

    @pytest.fixture(name="missing_table_parameters")
    def fixture_missing_table_parameters(self, sample_table: DbTable) -> dict:
        return dict(
            db_name=sample_table.db.name,
            schema_name=sample_table.schema,
            table_name="TABLE_THAT_DOES_NOT_EXIST",
        )

    @pytest.fixture(name="templated_table_parameters")
    def fixture_templated_table_parameters(self, sample_table: DbTable) -> dict:
        return dict(
            db_name=sample_table.db.name,
            schema_name=sample_table.schema,
            table_name="{{table_name}}",
        )


class TestDatasourceManualSubselect(
    dl_api_lib_testing.BaseTestDatasourceManualSubselect,
    OracleDatasetTestBase,
):
    source_type = SOURCE_TYPE_ORACLE_SUBSELECT
    conn_settings_cls = OracleConnectorSettings

    @pytest.fixture(name="parameters")
    def fixture_parameters(self, sample_table: DbTable) -> dict:
        return dict(subsql=f'select * from "{sample_table.name}"')


class TestDatasourceManualRawSqlLevelOff(
    dl_api_lib_testing.BaseTestDatasourceManualRawSqlLevelOff,
    OracleDatasetTestBase,
):
    source_type = SOURCE_TYPE_ORACLE_TABLE
    conn_settings_cls = OracleConnectorSettings

    @pytest.fixture(name="parameters")
    def fixture_parameters(self, sample_table: DbTable) -> dict:
        return dict(
            db_name=sample_table.db.name,
            schema_name=sample_table.schema,
            table_name=sample_table.name,
        )
