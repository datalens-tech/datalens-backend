from dl_api_connector.connection_info import ConnectionInfoProvider

from dl_connector_ydb.api.ydb.i18n.localizer import Translatable


class YDBConnectionInfoProvider(ConnectionInfoProvider):
    title_translatable = Translatable("label_connector-ydb")
