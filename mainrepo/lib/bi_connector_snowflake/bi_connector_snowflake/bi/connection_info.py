from bi_api_connector.connection_info import ConnectionInfoProvider

from bi_connector_snowflake.bi.i18n.localizer import Translatable


class SnowflakeConnectionInfoProvider(ConnectionInfoProvider):
    title_translatable = Translatable('label_connector-snowflake')
