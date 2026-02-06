import pytest

from dl_api_connector.i18n.localizer import CONFIGS as BI_API_CONNECTOR_CONFIGS
from dl_api_lib_testing.connection_form_base import (
    ConnectionFormTestBase,
    DatasourceTemplateConnectionFormTestMixin,
)

from dl_connector_mssql.api.connection_form.form_config import MSSQLConnectionFormFactory
from dl_connector_mssql.api.i18n.localizer import CONFIGS as BI_CONNECTOR_MSSQL_CONFIGS
from dl_connector_mssql.core.settings import MSSQLConnectorSettings


class TestMSSQLConnectionForm(
    ConnectionFormTestBase,
    DatasourceTemplateConnectionFormTestMixin,
):
    CONN_FORM_FACTORY_CLS = MSSQLConnectionFormFactory
    TRANSLATION_CONFIGS = BI_API_CONNECTOR_CONFIGS + BI_CONNECTOR_MSSQL_CONFIGS

    @pytest.fixture
    def connectors_settings(self, enable_datasource_template: bool) -> MSSQLConnectorSettings:
        return MSSQLConnectorSettings(
            ENABLE_DATASOURCE_TEMPLATE=enable_datasource_template,
        )
