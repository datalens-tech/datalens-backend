import pytest

import dl_api_lib_testing
from dl_core_testing.database import DbTable

from dl_connector_ydb.core.ydb.constants import (
    SOURCE_TYPE_YDB_SUBSELECT,
    SOURCE_TYPE_YDB_TABLE,
)
from dl_connector_ydb.core.ydb.settings import YDBConnectorSettings
from dl_connector_ydb_tests.db.api.base import YDBDatasetTestBase

_YDB_MANUAL_VALIDATES_TABLE = (
    "YDB validator always resolves the table path even when manual=True, so this invariant doesn't hold"
)


class TestDatasourceManualTable(
    dl_api_lib_testing.BaseTestDatasourceManualTable,
    YDBDatasetTestBase,
):
    source_type = SOURCE_TYPE_YDB_TABLE
    conn_settings_cls = YDBConnectorSettings

    @pytest.fixture(name="parameters")
    def fixture_parameters(self, sample_table: DbTable) -> dict:
        return {"table_name": sample_table.name}

    @pytest.fixture(name="missing_table_parameters")
    def fixture_missing_table_parameters(self, sample_table: DbTable) -> dict:
        return {"table_name": "table_that_does_not_exist"}

    @pytest.fixture(name="templated_table_parameters")
    def fixture_templated_table_parameters(self, sample_table: DbTable) -> dict:
        return {"table_name": "{{table_name}}"}

    @pytest.mark.skip(reason=_YDB_MANUAL_VALIDATES_TABLE)
    def test_manual_true_preserves_valid_for_nonexistent_table(self, *args, **kwargs) -> None: ...

    @pytest.mark.skip(reason=_YDB_MANUAL_VALIDATES_TABLE)
    def test_toggle_manual_false_to_true_resets_valid(self, *args, **kwargs) -> None: ...


class TestDatasourceManualSubselect(
    dl_api_lib_testing.BaseTestDatasourceManualSubselect,
    YDBDatasetTestBase,
):
    source_type = SOURCE_TYPE_YDB_SUBSELECT
    conn_settings_cls = YDBConnectorSettings

    @pytest.fixture(name="parameters")
    def fixture_parameters(self, sample_table: DbTable) -> dict:
        return {"subsql": f"select * from `{sample_table.name}`"}


class TestDatasourceManualRawSqlLevelOff(
    dl_api_lib_testing.BaseTestDatasourceManualRawSqlLevelOff,
    YDBDatasetTestBase,
):
    source_type = SOURCE_TYPE_YDB_TABLE
    conn_settings_cls = YDBConnectorSettings

    @pytest.fixture(name="parameters")
    def fixture_parameters(self, sample_table: DbTable) -> dict:
        return {"table_name": sample_table.name}
