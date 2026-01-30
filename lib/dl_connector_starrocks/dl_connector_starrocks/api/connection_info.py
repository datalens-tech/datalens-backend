from dl_api_connector.connection_info import ConnectionInfoProvider

from dl_connector_starrocks.api.i18n.localizer import Translatable


class StarRocksConnectionInfoProvider(ConnectionInfoProvider):
    title_translatable = Translatable("label_connector-starrocks")
