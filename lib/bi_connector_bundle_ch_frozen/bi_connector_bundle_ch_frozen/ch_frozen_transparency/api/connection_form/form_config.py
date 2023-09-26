from __future__ import annotations

from bi_connector_bundle_ch_frozen.ch_frozen_base.api.connection_form.form_config import CHFrozenBaseFormFactory
from bi_connector_bundle_ch_frozen.ch_frozen_transparency.api.connection_info import (
    CHFrozenTransparencyConnectionInfoProvider,
)


class CHFrozenTransparencyFormFactory(CHFrozenBaseFormFactory):
    def _title(self) -> str:
        return CHFrozenTransparencyConnectionInfoProvider.get_title(self._localizer)
