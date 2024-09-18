from dl_api_connector.i18n.localizer import CONFIGS as BI_API_CONNECTOR_CONFIGS
from dl_api_lib_testing.connection_form_base import ConnectionFormTestBase

from dl_connector_clickhouse.api.connection_form.form_config import ClickHouseConnectionFormFactory
from dl_connector_clickhouse.api.i18n.localizer import CONFIGS as BI_API_LIB_CONFIGS


class TestClickhouseConnectionForm(ConnectionFormTestBase):
    CONN_FORM_FACTORY_CLS = ClickHouseConnectionFormFactory
    TRANSLATION_CONFIGS = BI_API_CONNECTOR_CONFIGS + BI_API_LIB_CONFIGS
    OVERWRITE_EXPECTED_FORMS = False
