import pytest

from dl_api_connector.i18n.localizer import CONFIGS as BI_API_CONNECTOR_CONFIGS
from dl_api_lib_testing.connection_form_base import (
    ConnectionFormTestBase,
    DatasourceTemplateConnectionFormTestMixin,
)

from dl_connector_mysql.api.connection_form.form_config import MySQLConnectionFormFactory
from dl_connector_mysql.api.i18n.localizer import CONFIGS as BI_CONNECTOR_MYSQL_CONFIGS
from dl_connector_mysql.core.settings import MySQLConnectorSettings


class TestMySQLConnectionForm(
    ConnectionFormTestBase,
    DatasourceTemplateConnectionFormTestMixin,
):
    CONN_FORM_FACTORY_CLS = MySQLConnectionFormFactory
    TRANSLATION_CONFIGS = BI_API_CONNECTOR_CONFIGS + BI_CONNECTOR_MYSQL_CONFIGS

    @pytest.fixture
    def connectors_settings(self, enable_datasource_template: bool) -> MySQLConnectorSettings:
        return MySQLConnectorSettings(
            ENABLE_DATASOURCE_TEMPLATE=enable_datasource_template,
        )
