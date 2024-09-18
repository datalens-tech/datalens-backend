from dl_api_connector.i18n.localizer import CONFIGS as BI_API_CONNECTOR_CONFIGS
from dl_api_lib_testing.connection_form_base import ConnectionFormTestBase

from dl_connector_postgresql.api.connection_form.form_config import PostgreSQLConnectionFormFactory
from dl_connector_postgresql.api.i18n.localizer import CONFIGS as BI_CONNECTOR_POSTGRESQL_CONFIGS


class TestPostgresConnectionForm(ConnectionFormTestBase):
    CONN_FORM_FACTORY_CLS = PostgreSQLConnectionFormFactory
    TRANSLATION_CONFIGS = BI_API_CONNECTOR_CONFIGS + BI_CONNECTOR_POSTGRESQL_CONFIGS
    OVERWRITE_EXPECTED_FORMS = False
