import dl_api_lib_testing

from dl_connector_postgresql.core.postgresql.constants import SOURCE_TYPE_PG_SUBSELECT
from dl_connector_postgresql.core.postgresql.settings import PostgreSQLConnectorSettings
from dl_connector_postgresql_tests.db.api.base import (
    PostgreSQLDataApiTestBase,
    PostgreSQLDatasetTestBase,
)


class BaseSubselectTestSourceTemplate(dl_api_lib_testing.BaseSubselectTestSourceTemplate):
    source_type = SOURCE_TYPE_PG_SUBSELECT
    conn_settings_cls = PostgreSQLConnectorSettings


class TestControlApiSourceTemplate(
    BaseSubselectTestSourceTemplate,
    dl_api_lib_testing.BaseTestControlApiSourceTemplate,
    PostgreSQLDatasetTestBase,
):
    ...


class TestControlApiSourceTemplateSettingsDisabled(
    BaseSubselectTestSourceTemplate,
    dl_api_lib_testing.BaseTestControlApiSourceTemplateSettingsDisabled,
    PostgreSQLDatasetTestBase,
):
    ...


class TestControlApiSourceTemplateConnectionDisabled(
    BaseSubselectTestSourceTemplate,
    dl_api_lib_testing.BaseTestControlApiSourceTemplateConnectionDisabled,
    PostgreSQLDatasetTestBase,
):
    ...


class TestDataApiSourceTemplate(
    BaseSubselectTestSourceTemplate,
    dl_api_lib_testing.BaseTestDataApiSourceTemplate,
    PostgreSQLDataApiTestBase,
):
    ...
