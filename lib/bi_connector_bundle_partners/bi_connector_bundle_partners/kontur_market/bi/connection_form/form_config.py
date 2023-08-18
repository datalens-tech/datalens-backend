from __future__ import annotations

from typing import Optional

from bi_connector_bundle_partners.base.bi.connection_form.form_config import PartnersConnectionBaseFormFactory
from bi_connector_bundle_partners.base.bi.i18n.localizer import Translatable

from bi_connector_bundle_partners.kontur_market.bi.connection_info import KonturMarketConnectionInfoProvider


class KonturMarketConnectionFormFactory(PartnersConnectionBaseFormFactory):
    template_name = 'kontur_market'

    def _title(self) -> str:
        return KonturMarketConnectionInfoProvider.get_title(self._localizer)

    def _description(self) -> Optional[str]:
        return self._localizer.translate(Translatable('label_kontur_market_description'))
