import dl_api_lib_testing

from dl_connector_postgresql.core.postgresql.constants import SOURCE_TYPE_PG_SUBSELECT
from dl_connector_postgresql.core.postgresql.settings import PostgreSQLConnectorSettings
from dl_connector_postgresql_tests.db.api.base import PostgreSQLDataApiTestBase


class TestSysUserIdInSourceTemplate(
    dl_api_lib_testing.BaseTestDataApiSysUserIdSourceTemplate,
    PostgreSQLDataApiTestBase,
):
    source_type = SOURCE_TYPE_PG_SUBSELECT
    conn_settings_cls = PostgreSQLConnectorSettings
