from bi_api_connector.i18n.localizer import CONFIGS as BI_API_CONNECTOR_CONFIGS
from bi_api_lib_testing.connection_form_base import ConnectionFormTestBase

from bi_connector_clickhouse.bi.connection_form.form_config import ClickHouseConnectionFormFactory
from bi_connector_clickhouse.bi.i18n.localizer import CONFIGS as BI_API_LIB_CONFIGS


class TestClickhouseConnectionForm(ConnectionFormTestBase):
    CONN_FORM_FACTORY_CLS = ClickHouseConnectionFormFactory
    TRANSLATION_CONFIGS = BI_API_CONNECTOR_CONFIGS + BI_API_LIB_CONFIGS
