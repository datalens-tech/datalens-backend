import pytest

from dl_api_client.dsmaker.primitives import DataSource
import dl_api_lib_testing

from dl_connector_ydb.core.ydb.constants import (
    SOURCE_TYPE_YDB_SUBSELECT,
    SOURCE_TYPE_YDB_TABLE,
)
from dl_connector_ydb.core.ydb.settings import YDBConnectorSettings
from dl_connector_ydb_tests.db.api.base import (
    YDBDataApiTestBase,
    YDBDatasetTestBase,
)


class BaseTestSourceTemplate(dl_api_lib_testing.BaseTestSourceTemplate):
    conn_settings_cls = YDBConnectorSettings
    table_name_pattern = "test_table_.*"
    invalid_table_name = "test_table_invalid"


class BaseTableTestSourceTemplate(BaseTestSourceTemplate, dl_api_lib_testing.BaseTableTestSourceTemplate):
    source_type = SOURCE_TYPE_YDB_TABLE

    @pytest.fixture(name="datasource")
    def fixture_datasource(self, saved_connection_id: str) -> DataSource:
        parameters = dict(
            table_name="{{table_name}}",
            db_name=None,
            db_version=None,
        )

        return DataSource(
            connection_id=saved_connection_id,
            source_type=self.source_type.name,
            parameters=parameters,
        )

    @pytest.fixture(name="invalid_datasource")
    def fixture_invalid_datasource(self, saved_connection_id: str) -> DataSource:
        parameters = dict(
            table_name="{{invalid_parameter_name}}",
            db_name=None,
            db_version=None,
        )

        return DataSource(
            connection_id=saved_connection_id,
            source_type=self.source_type.name,
            parameters=parameters,
        )


class TestTableControlApiSourceTemplate(
    BaseTableTestSourceTemplate,
    dl_api_lib_testing.BaseTestControlApiSourceTemplate,
    YDBDatasetTestBase,
):
    ...


class TestTableControlApiSourceTemplateSettingsDisabled(
    BaseTableTestSourceTemplate,
    dl_api_lib_testing.BaseTestControlApiSourceTemplateSettingsDisabled,
    YDBDatasetTestBase,
):
    ...


class TestTableControlApiSourceTemplateConnectionDisabled(
    BaseTableTestSourceTemplate,
    dl_api_lib_testing.BaseTestControlApiSourceTemplateConnectionDisabled,
    YDBDatasetTestBase,
):
    ...


class TestTableDataApiSourceTemplate(
    BaseTableTestSourceTemplate,
    dl_api_lib_testing.BaseTestDataApiSourceTemplate,
    YDBDataApiTestBase,
):
    field_names = ["id"]


class BaseSubselectTestSourceTemplate(
    BaseTestSourceTemplate,
    dl_api_lib_testing.BaseSubselectTestSourceTemplate,
):
    source_type = SOURCE_TYPE_YDB_SUBSELECT


class TestSubselectControlApiSourceTemplate(
    BaseSubselectTestSourceTemplate,
    dl_api_lib_testing.BaseTestControlApiSourceTemplate,
    YDBDatasetTestBase,
):
    ...


class TestSubselectControlApiSourceTemplateSettingsDisabled(
    BaseSubselectTestSourceTemplate,
    dl_api_lib_testing.BaseTestControlApiSourceTemplateSettingsDisabled,
    YDBDatasetTestBase,
):
    ...


class TestSubselectControlApiSourceTemplateConnectionDisabled(
    BaseSubselectTestSourceTemplate,
    dl_api_lib_testing.BaseTestControlApiSourceTemplateConnectionDisabled,
    YDBDatasetTestBase,
):
    ...


class TestSubselectDataApiSourceTemplate(
    BaseSubselectTestSourceTemplate,
    dl_api_lib_testing.BaseTestDataApiSourceTemplate,
    YDBDataApiTestBase,
):
    field_names = ["id"]
