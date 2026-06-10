import pytest

from dl_api_connector.i18n.localizer import CONFIGS as BI_API_CONNECTOR_CONFIGS
from dl_api_lib_testing.connection_form_base import (
    ConnectionFormTestBase,
    RawSQLReadWriteConnectionFormTestMixin,
)

from dl_connector_trino.api.connection_form.form_config import TrinoConnectionFormFactory
from dl_connector_trino.api.i18n.localizer import CONFIGS as BI_CONNECTOR_TRINO_CONFIGS
from dl_connector_trino.core.settings import TrinoConnectorSettings


class TestTrinoConnectionForm(ConnectionFormTestBase, RawSQLReadWriteConnectionFormTestMixin):
    CONN_FORM_FACTORY_CLS = TrinoConnectionFormFactory
    TRANSLATION_CONFIGS = BI_API_CONNECTOR_CONFIGS + BI_CONNECTOR_TRINO_CONFIGS

    @pytest.fixture
    def connectors_settings(self, enable_raw_sql_readwrite: bool) -> TrinoConnectorSettings:
        return TrinoConnectorSettings(
            ENABLE_RAW_SQL_READWRITE_LEVEL=enable_raw_sql_readwrite,
        )
