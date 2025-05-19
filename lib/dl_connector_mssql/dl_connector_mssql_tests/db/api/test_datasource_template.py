import dl_api_lib_testing

from dl_connector_mssql.core.constants import SOURCE_TYPE_MSSQL_SUBSELECT
from dl_connector_mssql.core.settings import MSSQLConnectorSettings
from dl_connector_mssql_tests.db.api.base import (
    MSSQLDataApiTestBase,
    MSSQLDatasetTestBase,
)


class BaseSubselectTestSourceTemplate(dl_api_lib_testing.BaseSubselectTestSourceTemplate):
    source_type = SOURCE_TYPE_MSSQL_SUBSELECT
    conn_settings_cls = MSSQLConnectorSettings


class TestControlApiSourceTemplate(
    BaseSubselectTestSourceTemplate,
    dl_api_lib_testing.BaseTestControlApiSourceTemplate,
    MSSQLDatasetTestBase,
):
    ...


class TestControlApiSourceTemplateSettingsDisabled(
    BaseSubselectTestSourceTemplate,
    dl_api_lib_testing.BaseTestControlApiSourceTemplateSettingsDisabled,
    MSSQLDatasetTestBase,
):
    ...


class TestControlApiSourceTemplateConnectionDisabled(
    BaseSubselectTestSourceTemplate,
    dl_api_lib_testing.BaseTestControlApiSourceTemplateConnectionDisabled,
    MSSQLDatasetTestBase,
):
    ...


class TestDataApiSourceTemplate(
    BaseSubselectTestSourceTemplate,
    dl_api_lib_testing.BaseTestDataApiSourceTemplate,
    MSSQLDataApiTestBase,
):
    ...
