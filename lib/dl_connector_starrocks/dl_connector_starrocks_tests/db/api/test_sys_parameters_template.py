import dl_api_lib_testing

from dl_connector_starrocks.core.constants import SOURCE_TYPE_STARROCKS_SUBSELECT
from dl_connector_starrocks.core.settings import StarRocksConnectorSettings
from dl_connector_starrocks_tests.db.api.base import StarRocksDataApiTestBase


class TestSysUserIdInSourceTemplate(
    dl_api_lib_testing.BaseTestDataApiSysUserIdSourceTemplate,
    StarRocksDataApiTestBase,
):
    source_type = SOURCE_TYPE_STARROCKS_SUBSELECT
    conn_settings_cls = StarRocksConnectorSettings
