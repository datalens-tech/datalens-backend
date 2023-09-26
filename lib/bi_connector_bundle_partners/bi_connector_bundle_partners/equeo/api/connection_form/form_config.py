from __future__ import annotations

from bi_connector_bundle_partners.base.api.connection_form.form_config import PartnersConnectionBaseFormFactory
from bi_connector_bundle_partners.equeo.api.connection_info import EqueoConnectionInfoProvider


class EqueoConnectionFormFactory(PartnersConnectionBaseFormFactory):
    template_name = "equeo"

    def _title(self) -> str:
        return EqueoConnectionInfoProvider.get_title(self._localizer)
