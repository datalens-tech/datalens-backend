from dl_api_connector.i18n.localizer import CONFIGS as BI_API_CONNECTOR_CONFIGS
from dl_api_lib_testing.connection_form_base import ConnectionFormTestBase

from bi_connector_mssql.api.connection_form.form_config import MSSQLConnectionFormFactory
from bi_connector_mssql.api.i18n.localizer import CONFIGS as BI_CONNECTOR_MSSQL_CONFIGS


class TestMSSQLConnectionForm(ConnectionFormTestBase):
    CONN_FORM_FACTORY_CLS = MSSQLConnectionFormFactory
    TRANSLATION_CONFIGS = BI_API_CONNECTOR_CONFIGS + BI_CONNECTOR_MSSQL_CONFIGS
