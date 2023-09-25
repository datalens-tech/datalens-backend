from dl_api_connector.connection_info import ConnectionInfoProvider
from dl_connector_bigquery.api.i18n.localizer import Translatable


class BigQueryConnectionInfoProvider(ConnectionInfoProvider):
    title_translatable = Translatable("label_connector-bigquery")
