from dl_api_connector.connection_info import ConnectionInfoProvider

from bi_connector_metrica.bi.i18n.localizer import Translatable


class MetricaConnectionInfoProvider(ConnectionInfoProvider):
    title_translatable = Translatable("label_connector-metrica")
    alias = "metrica"


class AppMetricaConnectionInfoProvider(ConnectionInfoProvider):
    title_translatable = Translatable("label_connector-appmetrica")
    alias = "appmetrica"
