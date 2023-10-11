from dl_api_connector.connection_info import ConnectionInfoProvider

from dl_connector_oracle.api.i18n.localizer import Translatable


class OracleConnectionInfoProvider(ConnectionInfoProvider):
    title_translatable = Translatable("label_connector-oracle")
