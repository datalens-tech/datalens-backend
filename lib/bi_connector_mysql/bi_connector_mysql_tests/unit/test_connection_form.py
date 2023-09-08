from bi_api_lib_testing.connection_form_base import ConnectionFormTestBase
from bi_api_connector.i18n.localizer import CONFIGS as BI_API_CONNECTOR_CONFIGS

from bi_connector_mysql.bi.connection_form.form_config import MySQLConnectionFormFactory
from bi_connector_mysql.bi.i18n.localizer import CONFIGS as BI_CONNECTOR_MYSQL_CONFIGS


class TestMySQLConnectionForm(ConnectionFormTestBase):
    CONN_FORM_FACTORY_CLS = MySQLConnectionFormFactory
    TRANSLATION_CONFIGS = BI_API_CONNECTOR_CONFIGS + BI_CONNECTOR_MYSQL_CONFIGS
