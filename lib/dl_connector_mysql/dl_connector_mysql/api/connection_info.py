from dl_api_connector.connection_info import ConnectionInfoProvider

from dl_connector_mysql.api.i18n.localizer import Translatable


class MySQLConnectionInfoProvider(ConnectionInfoProvider):
    title_translatable = Translatable("label_connector-mysql")
