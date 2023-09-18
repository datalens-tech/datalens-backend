from typing import Optional

import pytest

from bi_api_commons_ya_cloud.models import (
    TenantYCFolder,
    TenantYCOrganization,
)
from dl_api_commons.base_models import TenantDef
from dl_api_connector.i18n.localizer import CONFIGS as BI_API_CONNECTOR_CONFIGS
from dl_api_lib_testing.connection_form_base import ConnectionFormTestBase
from dl_configs.connectors_settings import ConnectorSettingsBase

from bi_connector_mysql.bi.i18n.localizer import CONFIGS as BI_CONNECTOR_MYSQL_CONFIGS
from bi_connector_mysql_mdb.bi.connection_form.form_config import MySQLMDBConnectionFormFactory
from bi_connector_mysql_mdb.core.settings import MysqlConnectorSettings


class TestMySQLMDBConnectionForm(ConnectionFormTestBase):
    CONN_FORM_FACTORY_CLS = MySQLMDBConnectionFormFactory
    TRANSLATION_CONFIGS = BI_API_CONNECTOR_CONFIGS + BI_CONNECTOR_MYSQL_CONFIGS

    @pytest.fixture(
        params=(True, False),
        ids=("with_mdb", "no_mdb"),
    )
    def connectors_settings(self, request) -> Optional[ConnectorSettingsBase]:
        return MysqlConnectorSettings(USE_MDB_CLUSTER_PICKER=request.param)

    @pytest.fixture(
        params=(TenantYCFolder(folder_id="some_folder_id"), TenantYCOrganization(org_id="some_org_id")),
        ids=("in_folder", "in_org"),
    )
    def tenant(self, request) -> TenantDef:
        return request.param
