import pytest

from dl_api_connector.i18n.localizer import CONFIGS as BI_API_CONNECTOR_CONFIGS
from dl_api_lib_testing.connection_form_base import (
    ConnectionFormTestBase,
    DatasourceTemplateConnectionFormTestMixin,
)

from dl_connector_postgresql.api.connection_form.form_config import PostgreSQLConnectionFormFactory
from dl_connector_postgresql.api.i18n.localizer import CONFIGS as BI_CONNECTOR_POSTGRESQL_CONFIGS
from dl_connector_postgresql.core.postgresql.settings import PostgreSQLConnectorSettings


class TestPostgresConnectionForm(
    ConnectionFormTestBase,
    DatasourceTemplateConnectionFormTestMixin,
):
    CONN_FORM_FACTORY_CLS = PostgreSQLConnectionFormFactory
    TRANSLATION_CONFIGS = BI_API_CONNECTOR_CONFIGS + BI_CONNECTOR_POSTGRESQL_CONFIGS

    @pytest.fixture
    def connectors_settings(self, enable_datasource_template: bool) -> PostgreSQLConnectorSettings:
        return PostgreSQLConnectorSettings(
            ENABLE_DATASOURCE_TEMPLATE=enable_datasource_template,
        )
