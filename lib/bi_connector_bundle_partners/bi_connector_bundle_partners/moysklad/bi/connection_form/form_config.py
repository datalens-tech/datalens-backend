from __future__ import annotations

from typing import Optional

from dl_api_connector.form_config.models.common import MarkdownStr

from bi_connector_bundle_partners.base.bi.connection_form.form_config import PartnersConnectionBaseFormFactory
from bi_connector_bundle_partners.base.bi.i18n.localizer import Translatable
from bi_connector_bundle_partners.moysklad.bi.connection_info import MoySkladConnectionInfoProvider


DOC_LINK = "https://www.htmls.ru/learning/course/index.php?COURSE_ID=12&LESSON_ID=384&LESSON_PATH=380.381.384"


class MoySkladConnectionFormFactory(PartnersConnectionBaseFormFactory):
    template_name = "moysklad"

    def _title(self) -> str:
        return MoySkladConnectionInfoProvider.get_title(self._localizer)

    def _label_help_text(self) -> Optional[MarkdownStr]:
        return self._localizer.translate(Translatable("label_moysklad-token-hint")).format(DOC_LINK=DOC_LINK)
