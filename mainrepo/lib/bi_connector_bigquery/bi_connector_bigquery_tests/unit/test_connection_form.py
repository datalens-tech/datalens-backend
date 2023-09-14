from bi_api_connector.i18n.localizer import CONFIGS as BI_API_CONNECTOR_CONFIGS
from bi_api_lib_testing.connection_form_base import ConnectionFormTestBase

from bi_connector_bigquery.bi.connection_form.form_config import BigQueryConnectionFormFactory
from bi_connector_bigquery.bi.i18n.localizer import CONFIGS as BI_CONNECTOR_BIGQUERY_CONFIGS


class TestBigQueryConnectionForm(ConnectionFormTestBase):
    CONN_FORM_FACTORY_CLS = BigQueryConnectionFormFactory
    TRANSLATION_CONFIGS = BI_CONNECTOR_BIGQUERY_CONFIGS + BI_API_CONNECTOR_CONFIGS
