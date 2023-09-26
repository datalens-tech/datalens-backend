from __future__ import annotations

from bi_connector_bundle_ch_frozen.ch_frozen_base.api.connection_form.form_config import CHFrozenBaseFormFactory
from bi_connector_bundle_ch_frozen.ch_frozen_weather.api.connection_info import CHFrozenWeatherConnectionInfoProvider


class CHFrozenWeatherFormFactory(CHFrozenBaseFormFactory):
    def _title(self) -> str:
        return CHFrozenWeatherConnectionInfoProvider.get_title(self._localizer)
