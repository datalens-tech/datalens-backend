from __future__ import annotations

import abc
from typing import Optional

from dl_api_commons.base_models import TenantDef
from dl_api_connector.form_config.models.base import (
    ConnectionForm,
    ConnectionFormFactory,
    FormUIOverride,
)
import dl_api_connector.form_config.models.rows as C
from dl_configs.connectors_settings import ConnectorSettingsBase

from bi_connector_bundle_ch_frozen.ch_frozen_base.bi.i18n.localizer import Translatable


class CHFrozenBaseFormFactory(ConnectionFormFactory, metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def _title(self) -> str:
        raise NotImplementedError

    def _form_ui_override(self) -> Optional[FormUIOverride]:
        return None

    def get_form_config(
        self,
        connector_settings: Optional[ConnectorSettingsBase],
        tenant: Optional[TenantDef],
    ) -> ConnectionForm:
        text = self._localizer.translate(Translatable("label_ch-frozen-description"))
        return ConnectionForm(
            title=self._title(),
            rows=[
                C.CustomizableRow(items=[C.DescriptionRowItem(text=text)]),
            ],
            form_ui_override=self._form_ui_override(),
        )
