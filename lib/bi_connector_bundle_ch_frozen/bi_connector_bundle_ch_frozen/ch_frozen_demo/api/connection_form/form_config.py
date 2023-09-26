from __future__ import annotations

from typing import Optional

from dl_api_connector.form_config.models.base import FormUIOverride

from bi_connector_bundle_ch_frozen.ch_frozen_base.api.connection_form.form_config import CHFrozenBaseFormFactory
from bi_connector_bundle_ch_frozen.ch_frozen_demo.api.connection_info import CHFrozenDemoConnectionInfoProvider


class CHFrozenDemoFormFactory(CHFrozenBaseFormFactory):
    def _title(self) -> str:
        return CHFrozenDemoConnectionInfoProvider.get_title(self._localizer)

    def _form_ui_override(self) -> Optional[FormUIOverride]:
        return FormUIOverride(
            show_create_ql_chart_btn=True,
        )
