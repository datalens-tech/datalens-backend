from dl_api_connector.connection_info import ConnectionInfoProvider

from bi_connector_mssql.bi.i18n.localizer import Translatable


class MSSQLConnectionInfoProvider(ConnectionInfoProvider):
    title_translatable = Translatable("label_connector-mssql")
