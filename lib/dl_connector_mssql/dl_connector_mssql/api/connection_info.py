from dl_api_connector.connection_info import ConnectionInfoProvider

from dl_connector_mssql.api.i18n.localizer import Translatable


class MSSQLConnectionInfoProvider(ConnectionInfoProvider):
    title_translatable = Translatable("label_connector-mssql")
