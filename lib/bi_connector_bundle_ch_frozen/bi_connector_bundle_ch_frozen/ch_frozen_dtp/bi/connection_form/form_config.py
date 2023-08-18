from __future__ import annotations

from bi_connector_bundle_ch_frozen.ch_frozen_base.bi.connection_form.form_config import CHFrozenBaseFormFactory
from bi_connector_bundle_ch_frozen.ch_frozen_dtp.bi.connection_info import CHFrozenDTPConnectionInfoProvider


class CHFrozenDTPFormFactory(CHFrozenBaseFormFactory):
    def _title(self) -> str:
        return CHFrozenDTPConnectionInfoProvider.get_title(self._localizer)
