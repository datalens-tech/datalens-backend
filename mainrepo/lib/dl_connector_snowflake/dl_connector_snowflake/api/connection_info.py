from dl_api_connector.connection_info import ConnectionInfoProvider
from dl_connector_snowflake.api.i18n.localizer import Translatable


class SnowflakeConnectionInfoProvider(ConnectionInfoProvider):
    title_translatable = Translatable("label_connector-snowflake")
