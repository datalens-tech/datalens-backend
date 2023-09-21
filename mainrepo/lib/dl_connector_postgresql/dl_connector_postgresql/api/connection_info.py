from dl_api_connector.connection_info import ConnectionInfoProvider
from dl_connector_postgresql.api.i18n.localizer import Translatable


class PostgreSQLConnectionInfoProvider(ConnectionInfoProvider):
    title_translatable = Translatable("label_connector-postgres")
