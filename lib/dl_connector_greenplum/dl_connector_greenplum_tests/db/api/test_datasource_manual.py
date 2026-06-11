import pytest

import dl_api_lib_testing
from dl_core_testing.database import DbTable

from dl_connector_greenplum.core.constants import (
    SOURCE_TYPE_GP_SUBSELECT,
    SOURCE_TYPE_GP_TABLE,
)
from dl_connector_greenplum.core.settings import GreenplumConnectorSettings
from dl_connector_greenplum_tests.db.api.base import (
    GP6DatasetTestBase,
    GP7DatasetTestBase,
)


class _BaseTable(dl_api_lib_testing.BaseTestDatasourceManualTable):
    source_type = SOURCE_TYPE_GP_TABLE
    conn_settings_cls = GreenplumConnectorSettings

    @pytest.fixture(name="parameters")
    def fixture_parameters(self, sample_table: DbTable) -> dict:
        return {"schema_name": sample_table.schema, "table_name": sample_table.name}

    @pytest.fixture(name="missing_table_parameters")
    def fixture_missing_table_parameters(self, sample_table: DbTable) -> dict:
        return {"schema_name": sample_table.schema, "table_name": "table_that_does_not_exist"}

    @pytest.fixture(name="templated_table_parameters")
    def fixture_templated_table_parameters(self, sample_table: DbTable) -> dict:
        return {"schema_name": sample_table.schema, "table_name": "{{table_name}}"}


class _BaseSubselect(dl_api_lib_testing.BaseTestDatasourceManualSubselect):
    source_type = SOURCE_TYPE_GP_SUBSELECT
    conn_settings_cls = GreenplumConnectorSettings

    @pytest.fixture(name="parameters")
    def fixture_parameters(self, sample_table: DbTable) -> dict:
        return {"subsql": f'select * from "{sample_table.name}"'}


class _BaseRawSqlLevelOff(dl_api_lib_testing.BaseTestDatasourceManualRawSqlLevelOff):
    source_type = SOURCE_TYPE_GP_TABLE
    conn_settings_cls = GreenplumConnectorSettings

    @pytest.fixture(name="parameters")
    def fixture_parameters(self, sample_table: DbTable) -> dict:
        return {"schema_name": sample_table.schema, "table_name": sample_table.name}


class TestDatasourceManualTableGP6(_BaseTable, GP6DatasetTestBase): ...


class TestDatasourceManualTableGP7(_BaseTable, GP7DatasetTestBase): ...


class TestDatasourceManualSubselectGP6(_BaseSubselect, GP6DatasetTestBase): ...


class TestDatasourceManualSubselectGP7(_BaseSubselect, GP7DatasetTestBase): ...


class TestDatasourceManualRawSqlLevelOffGP6(_BaseRawSqlLevelOff, GP6DatasetTestBase): ...


class TestDatasourceManualRawSqlLevelOffGP7(_BaseRawSqlLevelOff, GP7DatasetTestBase): ...
