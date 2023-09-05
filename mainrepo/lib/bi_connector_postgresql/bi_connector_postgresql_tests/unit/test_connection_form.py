from bi_api_lib_testing.connection_form_base import ConnectionFormTestBase
from bi_api_connector.i18n.localizer import CONFIGS as BI_API_CONNECTOR_CONFIGS

from bi_connector_postgresql.bi.connection_form.form_config import PostgreSQLConnectionFormFactory
from bi_connector_postgresql.bi.i18n.localizer import CONFIGS as BI_CONNECTOR_POSTGRESQL_CONFIGS


class TestPostgresConnectionForm(ConnectionFormTestBase):
    CONN_FORM_FACTORY_CLS = PostgreSQLConnectionFormFactory
    TRANSLATION_CONFIGS = BI_API_CONNECTOR_CONFIGS + BI_CONNECTOR_POSTGRESQL_CONFIGS
