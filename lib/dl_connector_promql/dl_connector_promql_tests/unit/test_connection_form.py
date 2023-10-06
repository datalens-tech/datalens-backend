from dl_api_connector.i18n.localizer import CONFIGS as BI_API_CONNECTOR_CONFIGS
from dl_api_lib_testing.connection_form_base import ConnectionFormTestBase

from dl_connector_promql.api.connection_form.form_config import PromQLConnectionFormFactory
from dl_connector_promql.api.i18n.localizer import CONFIGS as BI_CONNECTOR_PROMQL_CONFIGS


class TestPromQLConnectionForm(ConnectionFormTestBase):
    CONN_FORM_FACTORY_CLS = PromQLConnectionFormFactory
    TRANSLATION_CONFIGS = BI_API_CONNECTOR_CONFIGS + BI_CONNECTOR_PROMQL_CONFIGS
