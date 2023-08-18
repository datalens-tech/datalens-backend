from __future__ import annotations

from bi_api_connector.connection_info import ConnectionInfoProvider
from bi_connector_bundle_ch_filtered_ya_cloud.base.bi.i18n.localizer import Translatable


class CHSMBHeatmapsConnectionInfoProvider(ConnectionInfoProvider):
    title_translatable = Translatable('label_connector-smb_heatmaps')
