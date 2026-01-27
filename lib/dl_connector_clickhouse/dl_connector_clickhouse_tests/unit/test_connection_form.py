import pytest

from dl_api_connector.i18n.localizer import CONFIGS as BI_API_CONNECTOR_CONFIGS
from dl_api_lib_testing.connection_form_base import ConnectionFormTestBase

from dl_connector_clickhouse.api.connection_form.form_config import ClickHouseConnectionFormFactory
from dl_connector_clickhouse.api.i18n.localizer import CONFIGS as BI_API_LIB_CONFIGS
from dl_connector_clickhouse.core.clickhouse.settings import ClickHouseConnectorSettings


class TestClickhouseConnectionForm(ConnectionFormTestBase):
    CONN_FORM_FACTORY_CLS = ClickHouseConnectionFormFactory
    TRANSLATION_CONFIGS = BI_API_CONNECTOR_CONFIGS + BI_API_LIB_CONFIGS

    @pytest.fixture(
        params=(True, False),
        ids=("with_template", "no_template"),
        name="enable_datasource_template",
    )
    def fixture_enable_datasource_template(self, request: pytest.FixtureRequest) -> bool:
        """Parametrize if a form has extra settings"""
        return request.param

    @pytest.fixture(name="connectors_settings")
    def fixture_connectors_settings(self, enable_datasource_template: bool) -> ClickHouseConnectorSettings | None:
        return ClickHouseConnectorSettings(
            ENABLE_DATASOURCE_TEMPLATE=enable_datasource_template,
        )
