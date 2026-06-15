import dl_api_lib_testing

from dl_connector_clickhouse.core.clickhouse.constants import SOURCE_TYPE_CH_SUBSELECT
from dl_connector_clickhouse.core.clickhouse.settings import ClickHouseConnectorSettings
from dl_connector_clickhouse_tests.db.api.base import ClickHouseDataApiTestBase


class TestSysUserIdInSourceTemplate(
    dl_api_lib_testing.BaseTestDataApiSysUserIdSourceTemplate,
    ClickHouseDataApiTestBase,
):
    source_type = SOURCE_TYPE_CH_SUBSELECT
    conn_settings_cls = ClickHouseConnectorSettings
