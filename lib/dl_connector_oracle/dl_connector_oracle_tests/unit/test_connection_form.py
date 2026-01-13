import pytest

from dl_api_connector.i18n.localizer import CONFIGS as BI_API_CONNECTOR_CONFIGS
from dl_api_lib_testing.connection_form_base import (
    ConnectionFormTestBase,
    DatasourceTemplateConnectionFormTestMixin,
)

from dl_connector_oracle.api.connection_form.form_config import OracleConnectionFormFactory
from dl_connector_oracle.api.i18n.localizer import CONFIGS as BI_CONNECTOR_ORACLE_CONFIGS
from dl_connector_oracle.core.settings import OracleConnectorSettings


class TestOracleConnectionForm(
    ConnectionFormTestBase,
    DatasourceTemplateConnectionFormTestMixin,
):
    CONN_FORM_FACTORY_CLS = OracleConnectionFormFactory
    TRANSLATION_CONFIGS = BI_CONNECTOR_ORACLE_CONFIGS + BI_API_CONNECTOR_CONFIGS

    @pytest.fixture
    def connectors_settings(self, enable_datasource_template: bool) -> OracleConnectorSettings:
        return OracleConnectorSettings(
            ENABLE_DATASOURCE_TEMPLATE=enable_datasource_template,
        )
