from __future__ import annotations

from typing import Optional

from bi_api_lib.connectors.solomon.connection_info import SolomonConnectionInfoProvider
from bi_configs.connectors_settings import ConnectorSettingsBase

from bi_api_commons.base_models import TenantDef

import bi_api_connector.form_config.models.rows as C
from bi_api_connector.form_config.models.base import ConnectionFormFactory, ConnectionForm, FormUIOverride

from bi_api_lib.i18n.localizer import Translatable


class SolomonConnectionFormFactory(ConnectionFormFactory):
    def get_form_config(
            self,
            connector_settings: Optional[ConnectorSettingsBase],
            tenant: Optional[TenantDef],
    ) -> ConnectionForm:
        description_text = self._localizer.translate(Translatable('label_solomon-conn-description')).format(
            LINK='https://monitoring.yandex-team.ru',
        )

        return ConnectionForm(
            title=SolomonConnectionInfoProvider.get_title(self._localizer),
            rows=[
                C.CustomizableRow(items=[
                    C.DescriptionRowItem(text=description_text),
                ])
            ],
            form_ui_override=FormUIOverride(
                show_create_dataset_btn=False,
            ),
        )
