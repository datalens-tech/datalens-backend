import pytest

import dl_api_lib_testing
from dl_core_testing.database import DbTable

from dl_connector_trino.core.constants import (
    SOURCE_TYPE_TRINO_SUBSELECT,
    SOURCE_TYPE_TRINO_TABLE,
)
from dl_connector_trino.core.settings import TrinoConnectorSettings
from dl_connector_trino_tests.db.api.base import TrinoDatasetTestBase


class TestDatasourceManualTable(
    dl_api_lib_testing.BaseTestDatasourceManualTable,
    TrinoDatasetTestBase,
):
    source_type = SOURCE_TYPE_TRINO_TABLE
    conn_settings_cls = TrinoConnectorSettings

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
            table_name="table_that_does_not_exist",
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
    TrinoDatasetTestBase,
):
    source_type = SOURCE_TYPE_TRINO_SUBSELECT
    conn_settings_cls = TrinoConnectorSettings

    @pytest.fixture(name="parameters")
    def fixture_parameters(self, sample_table: DbTable) -> dict:
        return dict(subsql=f"select * from {sample_table.db.name}.{sample_table.schema}.{sample_table.name}")


class TestDatasourceManualRawSqlLevelOff(
    dl_api_lib_testing.BaseTestDatasourceManualRawSqlLevelOff,
    TrinoDatasetTestBase,
):
    source_type = SOURCE_TYPE_TRINO_TABLE
    conn_settings_cls = TrinoConnectorSettings

    @pytest.fixture(name="parameters")
    def fixture_parameters(self, sample_table: DbTable) -> dict:
        return dict(
            db_name=sample_table.db.name,
            schema_name=sample_table.schema,
            table_name=sample_table.name,
        )
