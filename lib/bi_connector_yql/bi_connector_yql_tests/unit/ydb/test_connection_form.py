from typing import Optional

import pytest

from dl_configs.connectors_settings import ConnectorSettingsBase

from dl_api_lib_testing.connection_form_base import ConnectionFormTestBase
from dl_api_connector.i18n.localizer import CONFIGS as BI_API_CONNECTOR_CONFIGS

from bi_connector_yql.bi.ydb.connection_form.form_config import YDBConnectionFormFactory
from bi_connector_yql.bi.yql_base.i18n.localizer import CONFIGS as BI_CONNECTOR_YQL_CONFIGS
from bi_connector_yql.core.ydb.settings import YDBConnectorSettings


class TestYDBConnectionForm(ConnectionFormTestBase):
    CONN_FORM_FACTORY_CLS = YDBConnectionFormFactory
    TRANSLATION_CONFIGS = BI_API_CONNECTOR_CONFIGS + BI_CONNECTOR_YQL_CONFIGS

    @pytest.fixture(
        params=(True, False),
        ids=('with_mdb', 'no_mdb'),
    )
    def connectors_settings(self, request) -> Optional[ConnectorSettingsBase]:
        return YDBConnectorSettings(USE_MDB_CLUSTER_PICKER=request.param)
