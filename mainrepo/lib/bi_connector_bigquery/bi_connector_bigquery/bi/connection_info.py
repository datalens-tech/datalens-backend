from bi_api_connector.connection_info import ConnectionInfoProvider

from bi_connector_bigquery.bi.i18n.localizer import Translatable


class BigQueryConnectionInfoProvider(ConnectionInfoProvider):
    title_translatable = Translatable("label_connector-bigquery")
