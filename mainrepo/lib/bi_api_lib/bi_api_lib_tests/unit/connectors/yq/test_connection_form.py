from typing import Optional

import pytest

from bi_configs.connectors_settings import ConnectorSettingsBase, YQConnectorSettings

from bi_api_lib_testing.connection_form_base import ConnectionFormTestBase
from bi_api_connector.i18n.localizer import CONFIGS as BI_API_CONNECTOR_CONFIGS

from bi_api_lib.connectors.yq.connection_form.form_config import YQConnectionFormFactory
from bi_api_lib.i18n.localizer import CONFIGS as BI_API_LIB_CONFIGS


class TestYQConnectionForm(ConnectionFormTestBase):
    CONN_FORM_FACTORY_CLS = YQConnectionFormFactory
    TRANSLATION_CONFIGS = BI_API_CONNECTOR_CONFIGS + BI_API_LIB_CONFIGS

    @pytest.fixture(
        params=(True, False),
        ids=('with_mdb', 'no_mdb'),
    )
    def connectors_settings(self, request) -> Optional[ConnectorSettingsBase]:
        return YQConnectorSettings(
            USE_MDB_CLUSTER_PICKER=request.param,
            HOST='',
            PORT=0,
            DB_NAME='',
        )
