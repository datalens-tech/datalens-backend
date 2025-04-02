from typing import (
    ClassVar,
    Optional,
)

import pytest

from dl_api_connector.i18n.localizer import CONFIGS as DL_API_CONNECTOR_CONFIGS
from dl_api_lib_testing.connection_form_base import ConnectionFormTestBase
from dl_configs.connectors_settings import ConnectorSettingsBase

from dl_connector_ydb.api.ydb.connection_form.form_config import YDBConnectionFormFactory
from dl_connector_ydb.api.ydb.i18n.localizer import CONFIGS as DL_CONNECTOR_YDB_CONFIGS
from dl_connector_ydb.core.ydb.settings import YDBConnectorSettings


class TestYDBConnectionForm(ConnectionFormTestBase):
    CONN_FORM_FACTORY_CLS = YDBConnectionFormFactory
    TRANSLATION_CONFIGS = DL_API_CONNECTOR_CONFIGS + DL_CONNECTOR_YDB_CONFIGS
    OVERWRITE_EXPECTED_FORMS: ClassVar[bool] = False

    @pytest.fixture
    def connectors_settings(self) -> Optional[ConnectorSettingsBase]:
        return YDBConnectorSettings()
