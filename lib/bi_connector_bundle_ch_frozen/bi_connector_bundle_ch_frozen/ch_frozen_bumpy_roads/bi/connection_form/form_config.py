from __future__ import annotations

from bi_connector_bundle_ch_frozen.ch_frozen_base.bi.connection_form.form_config import CHFrozenBaseFormFactory
from bi_connector_bundle_ch_frozen.ch_frozen_bumpy_roads.bi.connection_info import (
    CHFrozenBumpyRoadsConnectionInfoProvider,
)


class CHFrozenBumpyRoadsFormFactory(CHFrozenBaseFormFactory):
    def _title(self) -> str:
        return CHFrozenBumpyRoadsConnectionInfoProvider.get_title(self._localizer)
