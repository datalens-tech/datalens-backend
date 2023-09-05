from typing import Optional

import pytest

from bi_configs.connectors_settings import ConnectorSettingsBase, PostgresConnectorSettings

from bi_api_commons.base_models import TenantDef
from bi_api_commons_ya_cloud.models import TenantYCFolder, TenantYCOrganization
from bi_api_lib_testing.connection_form_base import ConnectionFormTestBase
from bi_api_connector.i18n.localizer import CONFIGS as BI_API_CONNECTOR_CONFIGS

from bi_connector_postgresql.bi.i18n.localizer import CONFIGS as BI_CONNECTOR_POSTGRESQL_CONFIGS
from bi_connector_postgresql_mdb.bi.connection_form.form_config import PostgreSQLMDBConnectionFormFactory


class TestPostgresMDBConnectionForm(ConnectionFormTestBase):
    CONN_FORM_FACTORY_CLS = PostgreSQLMDBConnectionFormFactory
    TRANSLATION_CONFIGS = BI_API_CONNECTOR_CONFIGS + BI_CONNECTOR_POSTGRESQL_CONFIGS

    @pytest.fixture(
        params=(True, False),
        ids=('with_mdb', 'no_mdb'),
    )
    def connectors_settings(self, request) -> Optional[ConnectorSettingsBase]:
        return PostgresConnectorSettings(USE_MDB_CLUSTER_PICKER=request.param)

    @pytest.fixture(
        params=(TenantYCFolder(folder_id='some_folder_id'), TenantYCOrganization(org_id='some_org_id')),
        ids=('in_folder', 'in_org'),
    )
    def tenant(self, request) -> TenantDef:
        return request.param
