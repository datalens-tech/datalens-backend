from __future__ import annotations

from bi_connector_bundle_ch_frozen.ch_frozen_base.api.connection_form.form_config import CHFrozenBaseFormFactory
from bi_connector_bundle_ch_frozen.ch_frozen_samples.api.connection_info import CHFrozenSamplesConnectionInfoProvider


class CHFrozenSamplesFormFactory(CHFrozenBaseFormFactory):
    def _title(self) -> str:
        return CHFrozenSamplesConnectionInfoProvider.get_title(self._localizer)
