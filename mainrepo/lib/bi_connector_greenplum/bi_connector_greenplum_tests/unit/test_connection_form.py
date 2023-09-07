from bi_api_lib_testing.connection_form_base import ConnectionFormTestBase
from bi_api_connector.i18n.localizer import CONFIGS as BI_API_CONNECTOR_CONFIGS

from bi_connector_greenplum.bi.connection_form.form_config import GreenplumConnectionFormFactory
from bi_connector_greenplum.bi.i18n.localizer import CONFIGS as BI_CONNECTOR_GREENPLUM_CONFIGS


class TestGreenplumConnectionForm(ConnectionFormTestBase):
    CONN_FORM_FACTORY_CLS = GreenplumConnectionFormFactory
    TRANSLATION_CONFIGS = BI_API_CONNECTOR_CONFIGS + BI_CONNECTOR_GREENPLUM_CONFIGS
