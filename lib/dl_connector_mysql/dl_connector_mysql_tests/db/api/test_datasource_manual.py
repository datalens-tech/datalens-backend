import pytest

import dl_api_lib_testing
from dl_core_testing.database import DbTable

from dl_connector_mysql.core.constants import (
    SOURCE_TYPE_MYSQL_SUBSELECT,
    SOURCE_TYPE_MYSQL_TABLE,
)
from dl_connector_mysql.core.settings import MySQLConnectorSettings
from dl_connector_mysql_tests.db.api.base import MySQLDatasetTestBase


class TestDatasourceManualTable(
    dl_api_lib_testing.BaseTestDatasourceManualTable,
    MySQLDatasetTestBase,
):
    source_type = SOURCE_TYPE_MYSQL_TABLE
    conn_settings_cls = MySQLConnectorSettings

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
    MySQLDatasetTestBase,
):
    source_type = SOURCE_TYPE_MYSQL_SUBSELECT
    conn_settings_cls = MySQLConnectorSettings

    @pytest.fixture(name="parameters")
    def fixture_parameters(self, sample_table: DbTable) -> dict:
        return {"subsql": f"select * from {sample_table.db.name}.{sample_table.name}"}


class TestDatasourceManualRawSqlLevelOff(
    dl_api_lib_testing.BaseTestDatasourceManualRawSqlLevelOff,
    MySQLDatasetTestBase,
):
    source_type = SOURCE_TYPE_MYSQL_TABLE
    conn_settings_cls = MySQLConnectorSettings

    @pytest.fixture(name="parameters")
    def fixture_parameters(self, sample_table: DbTable) -> dict:
        return {"db_name": sample_table.db.name, "table_name": sample_table.name}
