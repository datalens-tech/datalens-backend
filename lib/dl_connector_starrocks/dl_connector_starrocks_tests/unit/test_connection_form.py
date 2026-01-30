import pytest

from dl_api_connector.i18n.localizer import CONFIGS as BI_API_CONNECTOR_CONFIGS
from dl_api_lib_testing.connection_form_base import (
    ConnectionFormTestBase,
    DatasourceTemplateConnectionFormTestMixin,
)

from dl_connector_starrocks.api.connection_form.form_config import StarRocksConnectionFormFactory
from dl_connector_starrocks.api.i18n.localizer import CONFIGS as BI_CONNECTOR_STARROCKS_CONFIGS
from dl_connector_starrocks.core.settings import DeprecatedStarRocksConnectorSettings


class TestStarRocksConnectionForm(
    ConnectionFormTestBase,
    DatasourceTemplateConnectionFormTestMixin,
):
    CONN_FORM_FACTORY_CLS = StarRocksConnectionFormFactory
    TRANSLATION_CONFIGS = BI_API_CONNECTOR_CONFIGS + BI_CONNECTOR_STARROCKS_CONFIGS

    @pytest.fixture
    def connectors_settings(self) -> DeprecatedStarRocksConnectorSettings:
        return DeprecatedStarRocksConnectorSettings()
