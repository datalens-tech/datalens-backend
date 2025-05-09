import pytest

from dl_api_connector.i18n.localizer import CONFIGS as DL_API_CONNECTOR_CONFIGS
from dl_api_lib_testing.connection_form_base import (
    ConnectionFormTestBase,
    DatasourceTemplateConnectionFormTestMixin,
)

from dl_connector_ydb.api.ydb.connection_form.form_config import YDBConnectionFormFactory
from dl_connector_ydb.api.ydb.i18n.localizer import CONFIGS as DL_CONNECTOR_YDB_CONFIGS
from dl_connector_ydb.core.ydb.settings import YDBConnectorSettings


class TestYDBConnectionForm(
    ConnectionFormTestBase,
    DatasourceTemplateConnectionFormTestMixin,
):
    CONN_FORM_FACTORY_CLS = YDBConnectionFormFactory
    TRANSLATION_CONFIGS = DL_API_CONNECTOR_CONFIGS + DL_CONNECTOR_YDB_CONFIGS

    @pytest.fixture
    def connectors_settings(self, enable_datasource_template: bool) -> YDBConnectorSettings:
        return YDBConnectorSettings(
            ENABLE_DATASOURCE_TEMPLATE=enable_datasource_template,
        )
