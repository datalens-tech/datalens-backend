from __future__ import annotations

from bi_connector_bundle_ch_frozen.ch_frozen_base.api.connection_form.form_config import CHFrozenBaseFormFactory
from bi_connector_bundle_ch_frozen.ch_frozen_gkh.api.connection_info import CHFrozenGKHConnectionInfoProvider


class CHFrozenGKHFormFactory(CHFrozenBaseFormFactory):
    def _title(self) -> str:
        return CHFrozenGKHConnectionInfoProvider.get_title(self._localizer)
