from bi_api_lib_testing.connection_form_base import ConnectionFormTestBase
from bi_api_connector.i18n.localizer import CONFIGS as BI_API_CONNECTOR_CONFIGS

from bi_api_lib.connectors.chydb.connection_form.form_config import CHYDBConnectionFormFactory
from bi_api_lib.i18n.localizer import CONFIGS as BI_API_LIB_CONFIGS


class TestCHYDBConnectionForm(ConnectionFormTestBase):
    CONN_FORM_FACTORY_CLS = CHYDBConnectionFormFactory
    TRANSLATION_CONFIGS = BI_API_CONNECTOR_CONFIGS + BI_API_LIB_CONFIGS
