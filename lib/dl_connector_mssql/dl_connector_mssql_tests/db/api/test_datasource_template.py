import dl_api_lib_testing

from dl_connector_mssql.core.constants import (
    SOURCE_TYPE_MSSQL_SUBSELECT,
    SOURCE_TYPE_MSSQL_TABLE,
)
from dl_connector_mssql.core.settings import MSSQLConnectorSettings
from dl_connector_mssql_tests.db.api.base import (
    MSSQLDataApiTestBase,
    MSSQLDatasetTestBase,
)


class BaseTableTestSourceTemplate(dl_api_lib_testing.BaseTableTestSourceTemplate):
    source_type = SOURCE_TYPE_MSSQL_TABLE
    conn_settings_cls = MSSQLConnectorSettings


class TestTableControlApiSourceTemplate(
    BaseTableTestSourceTemplate,
    dl_api_lib_testing.BaseTestControlApiSourceTemplate,
    MSSQLDatasetTestBase,
):
    ...


class TestTableControlApiSourceTemplateSettingsDisabled(
    BaseTableTestSourceTemplate,
    dl_api_lib_testing.BaseTestControlApiSourceTemplateSettingsDisabled,
    MSSQLDatasetTestBase,
):
    ...


class TestTableControlApiSourceTemplateConnectionDisabled(
    BaseTableTestSourceTemplate,
    dl_api_lib_testing.BaseTestControlApiSourceTemplateConnectionDisabled,
    MSSQLDatasetTestBase,
):
    ...


class TestTableDataApiSourceTemplate(
    BaseTableTestSourceTemplate,
    dl_api_lib_testing.BaseTestDataApiSourceTemplate,
    MSSQLDataApiTestBase,
):
    ...


class BaseSubselectTestSourceTemplate(dl_api_lib_testing.BaseSubselectTestSourceTemplate):
    source_type = SOURCE_TYPE_MSSQL_SUBSELECT
    conn_settings_cls = MSSQLConnectorSettings


class TestSubselectControlApiSourceTemplate(
    BaseSubselectTestSourceTemplate,
    dl_api_lib_testing.BaseTestControlApiSourceTemplate,
    MSSQLDatasetTestBase,
):
    ...


class TestSubselectControlApiSourceTemplateSettingsDisabled(
    BaseSubselectTestSourceTemplate,
    dl_api_lib_testing.BaseTestControlApiSourceTemplateSettingsDisabled,
    MSSQLDatasetTestBase,
):
    ...


class TestSubselectControlApiSourceTemplateConnectionDisabled(
    BaseSubselectTestSourceTemplate,
    dl_api_lib_testing.BaseTestControlApiSourceTemplateConnectionDisabled,
    MSSQLDatasetTestBase,
):
    ...


class TestSubselectDataApiSourceTemplate(
    BaseSubselectTestSourceTemplate,
    dl_api_lib_testing.BaseTestDataApiSourceTemplate,
    MSSQLDataApiTestBase,
):
    ...
