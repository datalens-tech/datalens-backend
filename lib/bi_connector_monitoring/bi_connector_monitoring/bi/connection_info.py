from __future__ import annotations

from dl_api_connector.connection_info import ConnectionInfoProvider

from bi_connector_monitoring.bi.i18n.localizer import Translatable


class MonitoringConnectionInfoProvider(ConnectionInfoProvider):
    title_translatable = Translatable("label_connector-monitoring")
