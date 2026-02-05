import pytest

from dl_api_connector.i18n.localizer import CONFIGS as BI_API_CONNECTOR_CONFIGS
from dl_api_lib_testing.connection_form_base import ConnectionFormTestBase

from dl_connector_clickhouse.api.connection_form.form_config import ClickHouseConnectionFormFactory
from dl_connector_clickhouse.api.i18n.localizer import CONFIGS as BI_API_LIB_CONFIGS
from dl_connector_clickhouse.core.clickhouse.settings import DeprecatedClickHouseConnectorSettings


class TestClickhouseConnectionForm(ConnectionFormTestBase):
    CONN_FORM_FACTORY_CLS = ClickHouseConnectionFormFactory
    TRANSLATION_CONFIGS = BI_API_CONNECTOR_CONFIGS + BI_API_LIB_CONFIGS

    @pytest.fixture(
        params=(True, False),
        ids=("with_template", "no_template"),
        name="enable_datasource_template",
    )
    def fixture_enable_datasource_template(self, request: pytest.FixtureRequest) -> bool:
        return request.param

    @pytest.fixture(
        params=(True, False),
        ids=("with_exp", "no_exp"),
        name="allow_experimental_features",
    )
    def fixture_allow_experimental_features(self, request: pytest.FixtureRequest) -> bool:
        return request.param

    @pytest.fixture(name="connectors_settings")
    def fixture_connectors_settings(
        self, enable_datasource_template: bool, allow_experimental_features: bool
    ) -> DeprecatedClickHouseConnectorSettings | None:
        return DeprecatedClickHouseConnectorSettings(
            ENABLE_DATASOURCE_TEMPLATE=enable_datasource_template,
            ALLOW_EXPERIMENTAL_FEATURES=allow_experimental_features,
        )
