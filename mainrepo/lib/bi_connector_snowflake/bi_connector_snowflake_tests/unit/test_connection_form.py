from bi_api_connector.i18n.localizer import CONFIGS as BI_API_CONNECTOR_CONFIGS
from bi_api_lib_testing.connection_form_base import ConnectionFormTestBase

from bi_connector_snowflake.bi.connection_form.form_config import SnowFlakeConnectionFormFactory
from bi_connector_snowflake.bi.i18n.localizer import CONFIGS as BI_CONNECTOR_SNOWFLAKE_CONFIGS


class TestSnowFlakeConnectionForm(ConnectionFormTestBase):
    CONN_FORM_FACTORY_CLS = SnowFlakeConnectionFormFactory
    TRANSLATION_CONFIGS = BI_CONNECTOR_SNOWFLAKE_CONFIGS + BI_API_CONNECTOR_CONFIGS
