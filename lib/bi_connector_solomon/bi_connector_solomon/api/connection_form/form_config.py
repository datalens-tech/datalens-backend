from __future__ import annotations

from typing import Optional

from dl_api_commons.base_models import TenantDef
from dl_api_connector.form_config.models.base import (
    ConnectionForm,
    ConnectionFormFactory,
    FormUIOverride,
)
import dl_api_connector.form_config.models.rows as C
from dl_configs.connectors_settings import ConnectorSettingsBase

from bi_connector_solomon.api.connection_info import SolomonConnectionInfoProvider
from bi_connector_solomon.api.i18n.localizer import Translatable


class SolomonConnectionFormFactory(ConnectionFormFactory):
    def get_form_config(
        self,
        connector_settings: Optional[ConnectorSettingsBase],
        tenant: Optional[TenantDef],
    ) -> ConnectionForm:
        description_text = self._localizer.translate(Translatable("label_solomon-conn-description")).format(
            LINK="https://monitoring.yandex-team.ru",
        )

        return ConnectionForm(
            title=SolomonConnectionInfoProvider.get_title(self._localizer),
            rows=[
                C.CustomizableRow(
                    items=[
                        C.DescriptionRowItem(text=description_text),
                    ]
                )
            ],
            form_ui_override=FormUIOverride(
                show_create_dataset_btn=False,
            ),
        )
