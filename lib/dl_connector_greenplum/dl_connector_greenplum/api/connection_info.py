from dl_api_connector.connection_info import ConnectionInfoProvider

from dl_connector_greenplum.api.i18n.localizer import Translatable


class GreenplumConnectionInfoProvider(ConnectionInfoProvider):
    title_translatable = Translatable("label_connector-greenplum")
