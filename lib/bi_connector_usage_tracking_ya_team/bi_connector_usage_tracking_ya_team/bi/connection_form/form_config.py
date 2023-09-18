from __future__ import annotations

from bi_connector_bundle_ch_filtered.base.bi.connection_form.form_config import ServiceConnectionBaseFormFactory
from bi_connector_usage_tracking_ya_team.bi.connection_info import UsageTrackingYaTeamConnectionInfoProvider
from bi_connector_usage_tracking_ya_team.bi.i18n.localizer import Translatable


class UsageTrackingYaTeamConnectionFormFactory(ServiceConnectionBaseFormFactory):
    template_name = "usage_tracking_ya_team"

    def _title(self) -> str:
        return UsageTrackingYaTeamConnectionInfoProvider.get_title(self._localizer)

    def _description(self) -> str:
        return self._localizer.translate(Translatable("label_utyt-conn-description"))
