import dl_api_lib_testing

from dl_connector_mysql.core.constants import SOURCE_TYPE_MYSQL_SUBSELECT
from dl_connector_mysql.core.settings import MySQLConnectorSettings
from dl_connector_mysql_tests.db.api.base import MySQLDataApiTestBase


class TestSysUserIdInSourceTemplate(
    dl_api_lib_testing.BaseTestDataApiSysUserIdSourceTemplate,
    MySQLDataApiTestBase,
):
    source_type = SOURCE_TYPE_MYSQL_SUBSELECT
    conn_settings_cls = MySQLConnectorSettings
