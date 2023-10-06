from dl_api_connector.connection_info import ConnectionInfoProvider

from dl_connector_promql.api.i18n.localizer import Translatable


class PromQLConnectionInfoProvider(ConnectionInfoProvider):
    title_translatable = Translatable("label_connector-promql")
