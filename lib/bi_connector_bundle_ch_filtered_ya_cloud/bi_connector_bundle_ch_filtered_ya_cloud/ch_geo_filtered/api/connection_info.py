from dl_api_connector.connection_info import ConnectionInfoProvider

from bi_connector_bundle_ch_filtered_ya_cloud.base.api.i18n.localizer import Translatable


class CHGeoFilteredConnectionInfoProvider(ConnectionInfoProvider):
    title_translatable = Translatable("label_connector-ch_geo_filtered")
