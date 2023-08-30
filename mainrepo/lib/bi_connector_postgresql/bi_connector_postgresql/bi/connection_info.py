from bi_api_connector.connection_info import ConnectionInfoProvider
from bi_connector_postgresql.bi.i18n.localizer import Translatable


class PostgreSQLConnectionInfoProvider(ConnectionInfoProvider):
    title_translatable = Translatable('label_connector-postgres')
