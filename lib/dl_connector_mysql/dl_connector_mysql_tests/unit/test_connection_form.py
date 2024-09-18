from dl_api_connector.i18n.localizer import CONFIGS as BI_API_CONNECTOR_CONFIGS
from dl_api_lib_testing.connection_form_base import ConnectionFormTestBase

from dl_connector_mysql.api.connection_form.form_config import MySQLConnectionFormFactory
from dl_connector_mysql.api.i18n.localizer import CONFIGS as BI_CONNECTOR_MYSQL_CONFIGS


class TestMySQLConnectionForm(ConnectionFormTestBase):
    CONN_FORM_FACTORY_CLS = MySQLConnectionFormFactory
    TRANSLATION_CONFIGS = BI_API_CONNECTOR_CONFIGS + BI_CONNECTOR_MYSQL_CONFIGS
    OVERWRITE_EXPECTED_FORMS = False
