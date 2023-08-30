from __future__ import annotations

import abc
from typing import Optional, ClassVar

from bi_configs.connectors_settings import ConnectorSettingsBase

from bi_api_commons.base_models import TenantDef

import bi_api_connector.form_config.models.rows as C
from bi_api_connector.form_config.models.shortcuts.rows import RowConstructor
from bi_api_connector.form_config.models.api_schema import (
    FormActionApiSchema,
    FormFieldApiSchema,
    FormApiSchema,
)
from bi_api_connector.form_config.models.base import ConnectionFormFactory, ConnectionForm, ConnectionFormMode
from bi_api_connector.form_config.models.rows.base import FormRow
from bi_api_connector.form_config.models.common import CommonFieldName, MarkdownStr


class PartnersConnectionBaseFormFactory(ConnectionFormFactory, metaclass=abc.ABCMeta):
    template_name: ClassVar[str]

    @abc.abstractmethod
    def _title(self) -> str:
        raise NotImplementedError

    def _description(self) -> Optional[str]:
        return None

    def _label_help_text(self) -> Optional[MarkdownStr]:
        return None

    def get_form_config(
            self,
            connector_settings: Optional[ConnectorSettingsBase],
            tenant: Optional[TenantDef],
    ) -> ConnectionForm:
        rc = RowConstructor(localizer=self._localizer)

        rows: list[FormRow] = []
        create_api_schema: Optional[FormActionApiSchema] = None

        if (desc_item := self._description()) is not None:
            rows.append(C.CustomizableRow(items=[
                C.DescriptionRowItem(text=desc_item)
            ]))

        if self.mode == ConnectionFormMode.create:
            rows.extend([
                rc.access_token_input_row(mode=self.mode, label_help_text=self._label_help_text()),
                rc.auto_create_dash_row(),
            ])

            create_api_schema = FormActionApiSchema(
                items=[
                    *self._get_top_level_create_api_schema_items(),
                    FormFieldApiSchema(name=CommonFieldName.access_token, required=True),
                ],
            )

        return ConnectionForm(
            title=self._title(),
            template_name=self.template_name,
            rows=rows,
            api_schema=FormApiSchema(create=create_api_schema),
        )
