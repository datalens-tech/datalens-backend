import dl_api_lib_testing

from dl_connector_ydb.core.ydb.constants import SOURCE_TYPE_YDB_SUBSELECT
from dl_connector_ydb.core.ydb.settings import YDBConnectorSettings
from dl_connector_ydb_tests.db.api.base import YDBDataApiTestBase


class TestSysUserIdInSourceTemplate(
    dl_api_lib_testing.BaseTestDataApiSysUserIdSourceTemplate,
    YDBDataApiTestBase,
):
    source_type = SOURCE_TYPE_YDB_SUBSELECT
    conn_settings_cls = YDBConnectorSettings
