from __future__ import annotations

from bi_api_connector.connection_info import ConnectionInfoProvider
from bi_connector_usage_tracking_ya_team.bi.i18n.localizer import Translatable


class UsageTrackingYaTeamConnectionInfoProvider(ConnectionInfoProvider):
    title_translatable = Translatable('label_connector-usage_tracking_ya_team')
