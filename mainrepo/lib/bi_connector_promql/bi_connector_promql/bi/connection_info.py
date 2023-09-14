from bi_api_connector.connection_info import ConnectionInfoProvider

from bi_connector_promql.bi.i18n.localizer import Translatable


class PromQLConnectionInfoProvider(ConnectionInfoProvider):
    title_translatable = Translatable("label_connector-promql")
