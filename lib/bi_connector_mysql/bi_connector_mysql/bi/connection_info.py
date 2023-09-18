from dl_api_connector.connection_info import ConnectionInfoProvider

from bi_connector_mysql.bi.i18n.localizer import Translatable


class MySQLConnectionInfoProvider(ConnectionInfoProvider):
    title_translatable = Translatable('label_connector-mysql')
