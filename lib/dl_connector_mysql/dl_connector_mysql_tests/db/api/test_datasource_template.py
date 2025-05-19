import dl_api_lib_testing

from dl_connector_mysql.core.constants import SOURCE_TYPE_MYSQL_SUBSELECT
from dl_connector_mysql.core.settings import MySQLConnectorSettings
from dl_connector_mysql_tests.db.api.base import (
    MySQLDataApiTestBase,
    MySQLDatasetTestBase,
)


class BaseSubselectTestSourceTemplate(dl_api_lib_testing.BaseSubselectTestSourceTemplate):
    source_type = SOURCE_TYPE_MYSQL_SUBSELECT
    conn_settings_cls = MySQLConnectorSettings


class TestControlApiSourceTemplate(
    BaseSubselectTestSourceTemplate,
    dl_api_lib_testing.BaseTestControlApiSourceTemplate,
    MySQLDatasetTestBase,
):
    ...


class TestControlApiSourceTemplateSettingsDisabled(
    BaseSubselectTestSourceTemplate,
    dl_api_lib_testing.BaseTestControlApiSourceTemplateSettingsDisabled,
    MySQLDatasetTestBase,
):
    ...


class TestControlApiSourceTemplateConnectionDisabled(
    BaseSubselectTestSourceTemplate,
    dl_api_lib_testing.BaseTestControlApiSourceTemplateConnectionDisabled,
    MySQLDatasetTestBase,
):
    ...


class TestDataApiSourceTemplate(
    BaseSubselectTestSourceTemplate,
    dl_api_lib_testing.BaseTestDataApiSourceTemplate,
    MySQLDataApiTestBase,
):
    ...
