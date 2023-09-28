from dl_api_connector.i18n.localizer import CONFIGS as BI_API_CONNECTOR_CONFIGS
from dl_api_lib_testing.connection_form_base import ConnectionFormTestBase

from bi_connector_gsheets.api.connection_form.form_config import GSheetsConnectionFormFactory
from bi_connector_gsheets.api.i18n.localizer import CONFIGS as BI_CONNECTOR_GSHEETS_CONFIGS


class TestGSheetsConnectionForm(ConnectionFormTestBase):
    CONN_FORM_FACTORY_CLS = GSheetsConnectionFormFactory
    TRANSLATION_CONFIGS = BI_API_CONNECTOR_CONFIGS + BI_CONNECTOR_GSHEETS_CONFIGS
