from dl_api_connector.i18n.localizer import CONFIGS as BI_API_CONNECTOR_CONFIGS
from dl_api_lib_testing.connection_form_base import ConnectionFormTestBase

from dl_connector_oracle.api.connection_form.form_config import OracleConnectionFormFactory
from dl_connector_oracle.api.i18n.localizer import CONFIGS as BI_CONNECTOR_ORACLE_CONFIGS


class TestOracleConnectionForm(ConnectionFormTestBase):
    CONN_FORM_FACTORY_CLS = OracleConnectionFormFactory
    TRANSLATION_CONFIGS = BI_CONNECTOR_ORACLE_CONFIGS + BI_API_CONNECTOR_CONFIGS
