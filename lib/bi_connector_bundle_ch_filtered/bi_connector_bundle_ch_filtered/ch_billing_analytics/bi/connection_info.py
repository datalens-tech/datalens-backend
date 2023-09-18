from __future__ import annotations

from dl_api_connector.connection_info import ConnectionInfoProvider

from bi_connector_bundle_ch_filtered.base.bi.i18n.localizer import Translatable


class CHBillingAnalyticsConnectionInfoProvider(ConnectionInfoProvider):
    alias = "yc_billing"
    title_translatable = Translatable("label_connector-ch_billing_analytics")
