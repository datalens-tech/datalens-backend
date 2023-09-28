from dl_api_connector.i18n.localizer import CONFIGS as BI_API_CONNECTOR_CONFIGS
from dl_api_lib_testing.connection_form_base import ConnectionFormTestBase

from bi_connector_solomon.api.connection_form.form_config import SolomonConnectionFormFactory
from bi_connector_solomon.api.i18n.localizer import CONFIGS as BI_CONNECTOR_SOLOMON_CONFIGS


class TestSolomonConnectionForm(ConnectionFormTestBase):
    CONN_FORM_FACTORY_CLS = SolomonConnectionFormFactory
    TRANSLATION_CONFIGS = BI_API_CONNECTOR_CONFIGS + BI_CONNECTOR_SOLOMON_CONFIGS
