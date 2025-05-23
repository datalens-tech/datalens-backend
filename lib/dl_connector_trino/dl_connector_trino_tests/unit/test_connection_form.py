from dl_api_connector.i18n.localizer import CONFIGS as BI_API_CONNECTOR_CONFIGS
from dl_api_lib_testing.connection_form_base import ConnectionFormTestBase

from dl_connector_trino.api.connection_form.form_config import TrinoConnectionFormFactory
from dl_connector_trino.api.i18n.localizer import CONFIGS as BI_CONNECTOR_TRINO_CONFIGS


class TestTrinoConnectionForm(ConnectionFormTestBase):
    CONN_FORM_FACTORY_CLS = TrinoConnectionFormFactory
    TRANSLATION_CONFIGS = BI_API_CONNECTOR_CONFIGS + BI_CONNECTOR_TRINO_CONFIGS
