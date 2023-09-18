from dl_api_connector.connection_info import ConnectionInfoProvider

from bi_connector_oracle.bi.i18n.localizer import Translatable


class OracleConnectionInfoProvider(ConnectionInfoProvider):
    title_translatable = Translatable('label_connector-oracle')
