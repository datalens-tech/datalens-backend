from __future__ import annotations

from bi_connector_bundle_ch_frozen.ch_frozen_base.bi.connection_form.form_config import CHFrozenBaseFormFactory
from bi_connector_bundle_ch_frozen.ch_frozen_covid.bi.connection_info import CHFrozenCovidConnectionInfoProvider


class CHFrozenCovidFormFactory(CHFrozenBaseFormFactory):
    def _title(self) -> str:
        return CHFrozenCovidConnectionInfoProvider.get_title(self._localizer)
