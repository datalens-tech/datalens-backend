from __future__ import annotations

from bi_connector_bundle_ch_filtered.base.api.connection_form.form_config import (
    ServiceConnectionWithTokenBaseFormFactory,
)
from bi_connector_bundle_ch_filtered_ya_cloud.base.api.i18n.localizer import Translatable


class CHSMBHeatmapsConnectionFormFactory(ServiceConnectionWithTokenBaseFormFactory):
    template_name = "smb_heatmaps"

    def _title(self) -> str:
        return self._localizer.translate(Translatable("label_connector-smb_heatmaps"))

    def _description(self) -> str:
        return self._localizer.translate(Translatable("label_smb-heatmaps-description"))
