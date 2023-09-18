from __future__ import annotations

from bi_connector_bundle_ch_filtered.base.bi.connection_form.form_config import (
    ServiceConnectionWithTokenBaseFormFactory,
)
from bi_connector_bundle_ch_filtered_ya_cloud.base.bi.i18n.localizer import Translatable
from bi_connector_bundle_ch_filtered_ya_cloud.schoolbook.bi.connection_info import CHSchoolbookConnectionInfoProvider


class CHSchoolbookConnectionFormFactory(ServiceConnectionWithTokenBaseFormFactory):
    template_name = "schoolbook_journal"

    def _title(self) -> str:
        return CHSchoolbookConnectionInfoProvider.get_title(self._localizer)

    def _description(self) -> str:
        docs_url = self._localizer.translate(Translatable("cloud-datalens-docs-url"))
        docs_url += "/security"

        passport_url = "https://passport.yandex.ru/profile"

        return self._localizer.translate(Translatable("label_schoolbook-journal-description")).format(
            SEC_LINK=docs_url,
            PASSPORT_LINK=passport_url,
        )
