from __future__ import annotations

from dl_api_connector.connection_info import ConnectionInfoProvider

from bi_connector_bundle_ch_filtered.base.api.i18n.localizer import Translatable


class UsageTrackingConnectionInfoProvider(ConnectionInfoProvider):
    title_translatable = Translatable("label_connector-usage_tracking")
