import dl_api_lib_testing

from dl_connector_postgresql.core.postgresql.constants import (
    SOURCE_TYPE_PG_SUBSELECT,
    SOURCE_TYPE_PG_TABLE,
)
from dl_connector_postgresql.core.postgresql.settings import PostgreSQLConnectorSettings
from dl_connector_postgresql_tests.db.api.base import (
    PostgreSQLDataApiTestBase,
    PostgreSQLDatasetTestBase,
)


class BaseTableTestSourceTemplate(dl_api_lib_testing.BaseTableTestSourceTemplate):
    source_type = SOURCE_TYPE_PG_TABLE
    conn_settings_cls = PostgreSQLConnectorSettings


class TestTableControlApiSourceTemplate(
    BaseTableTestSourceTemplate,
    dl_api_lib_testing.BaseTestControlApiSourceTemplate,
    PostgreSQLDatasetTestBase,
):
    ...


class TestTableControlApiSourceTemplateSettingsDisabled(
    BaseTableTestSourceTemplate,
    dl_api_lib_testing.BaseTestControlApiSourceTemplateSettingsDisabled,
    PostgreSQLDatasetTestBase,
):
    ...


class TestTableControlApiSourceTemplateConnectionDisabled(
    BaseTableTestSourceTemplate,
    dl_api_lib_testing.BaseTestControlApiSourceTemplateConnectionDisabled,
    PostgreSQLDatasetTestBase,
):
    ...


class TestTableDataApiSourceTemplate(
    BaseTableTestSourceTemplate,
    dl_api_lib_testing.BaseTestDataApiSourceTemplate,
    PostgreSQLDataApiTestBase,
):
    ...


class BaseSubselectTestSourceTemplate(dl_api_lib_testing.BaseSubselectTestSourceTemplate):
    source_type = SOURCE_TYPE_PG_SUBSELECT
    conn_settings_cls = PostgreSQLConnectorSettings


class TestSubselectControlApiSourceTemplate(
    BaseSubselectTestSourceTemplate,
    dl_api_lib_testing.BaseTestControlApiSourceTemplate,
    PostgreSQLDatasetTestBase,
):
    ...


class TestSubselectControlApiSourceTemplateSettingsDisabled(
    BaseSubselectTestSourceTemplate,
    dl_api_lib_testing.BaseTestControlApiSourceTemplateSettingsDisabled,
    PostgreSQLDatasetTestBase,
):
    ...


class TestSubselectControlApiSourceTemplateConnectionDisabled(
    BaseSubselectTestSourceTemplate,
    dl_api_lib_testing.BaseTestControlApiSourceTemplateConnectionDisabled,
    PostgreSQLDatasetTestBase,
):
    ...


class TestSubselectDataApiSourceTemplate(
    BaseSubselectTestSourceTemplate,
    dl_api_lib_testing.BaseTestDataApiSourceTemplate,
    PostgreSQLDataApiTestBase,
):
    ...
