from dl_api_connector.connection_info import ConnectionInfoProvider

from dl_connector_clickhouse.api.i18n.localizer import Translatable


class ClickHouseConnectionInfoProvider(ConnectionInfoProvider):
    title_translatable = Translatable("label_connector-clickhouse")
