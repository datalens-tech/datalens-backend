from bi_api_lib_testing.connection_form_base import ConnectionFormTestBase
from bi_api_connector.i18n.localizer import CONFIGS as BI_API_CONNECTOR_CONFIGS

from bi_connector_oracle.bi.connection_form.form_config import OracleConnectionFormFactory
from bi_connector_oracle.bi.i18n.localizer import CONFIGS as BI_CONNECTOR_ORACLE_CONFIGS


class TestOracleConnectionForm(ConnectionFormTestBase):
    CONN_FORM_FACTORY_CLS = OracleConnectionFormFactory
    TRANSLATION_CONFIGS = BI_CONNECTOR_ORACLE_CONFIGS + BI_API_CONNECTOR_CONFIGS
