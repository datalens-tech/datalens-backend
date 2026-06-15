import dl_api_lib_testing

from dl_connector_mssql.core.constants import SOURCE_TYPE_MSSQL_SUBSELECT
from dl_connector_mssql.core.settings import MSSQLConnectorSettings
from dl_connector_mssql_tests.db.api.base import MSSQLDataApiTestBase


class TestSysUserIdInSourceTemplate(
    dl_api_lib_testing.BaseTestDataApiSysUserIdSourceTemplate,
    MSSQLDataApiTestBase,
):
    source_type = SOURCE_TYPE_MSSQL_SUBSELECT
    conn_settings_cls = MSSQLConnectorSettings
