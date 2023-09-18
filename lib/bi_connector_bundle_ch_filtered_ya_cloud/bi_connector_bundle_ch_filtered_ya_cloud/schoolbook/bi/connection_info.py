from __future__ import annotations

from dl_api_connector.connection_info import ConnectionInfoProvider

from bi_connector_bundle_ch_filtered_ya_cloud.base.bi.i18n.localizer import Translatable


class CHSchoolbookConnectionInfoProvider(ConnectionInfoProvider):
    title_translatable = Translatable("label_connector-schoolbook_journal")
