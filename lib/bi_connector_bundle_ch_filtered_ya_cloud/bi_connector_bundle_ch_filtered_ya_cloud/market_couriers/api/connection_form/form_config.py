from __future__ import annotations

from bi_connector_bundle_ch_filtered.base.api.connection_form.form_config import (
    ServiceConnectionWithTokenBaseFormFactory,
)
from bi_connector_bundle_ch_filtered_ya_cloud.base.api.i18n.localizer import Translatable
from bi_connector_bundle_ch_filtered_ya_cloud.market_couriers.api.connection_info import (
    CHMarketCouriersConnectionInfoProvider,
)


class CHMarketCouriersConnectionFormFactory(ServiceConnectionWithTokenBaseFormFactory):
    template_name = "market_couriers"

    def _title(self) -> str:
        return CHMarketCouriersConnectionInfoProvider.get_title(self._localizer)

    def _description(self) -> str:
        docs_url = self._localizer.translate(Translatable("cloud-datalens-docs-url"))
        docs_url += "/security"

        passport_url = "https://passport.yandex.ru/profile"

        return self._localizer.translate(Translatable("label_market-couriers-description")).format(
            SEC_LINK=docs_url,
            PASSPORT_LINK=passport_url,
        )
