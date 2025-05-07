
import dl_api_lib_testing

from dl_connector_clickhouse.core.clickhouse.constants import SOURCE_TYPE_CH_SUBSELECT
from dl_connector_clickhouse.core.clickhouse.settings import ClickHouseConnectorSettings
from dl_connector_clickhouse_tests.db.api.base import ClickHouseDatasetTestBase


class BaseSubselectTestSourceTemplate(dl_api_lib_testing.BaseSubselectTestSourceTemplate, ClickHouseDatasetTestBase):
    source_type = SOURCE_TYPE_CH_SUBSELECT
    conn_settings_cls = ClickHouseConnectorSettings


class TestControlApiSourceTemplate(
    BaseSubselectTestSourceTemplate,
    dl_api_lib_testing.BaseTestControlApiSourceTemplate,
):
    ...


class TestControlApiSourceTemplateSettingsDisabled(
    BaseSubselectTestSourceTemplate,
    dl_api_lib_testing.BaseTestControlApiSourceTemplateSettingsDisabled,
):
    ...


class TestControlApiSourceTemplateConnectionDisabled(
    BaseSubselectTestSourceTemplate,
    dl_api_lib_testing.BaseTestControlApiSourceTemplateConnectionDisabled,
):
    ...
