from __future__ import annotations

from enum import unique
from typing import Optional

from dl_api_commons.base_models import TenantDef
from dl_api_connector.form_config.models.api_schema import (
    FormActionApiSchema,
    FormApiSchema,
    FormFieldApiSchema,
)
from dl_api_connector.form_config.models.base import (
    ConnectionForm,
    ConnectionFormFactory,
    ConnectionFormMode,
)
from dl_api_connector.form_config.models.common import (
    CommonFieldName,
    FormFieldName,
)
import dl_api_connector.form_config.models.rows as C
from dl_api_connector.form_config.models.rows.base import FormRow
from dl_api_connector.form_config.models.shortcuts.rows import RowConstructor
from dl_configs.connectors_settings import ConnectorSettingsBase

from dl_connector_bitrix_gds.api.connection_info import BitrixGDSConnectionInfoProvider
from dl_connector_bitrix_gds.api.i18n.localizer import Translatable


@unique
class BitrixGDSFieldName(FormFieldName):
    portal = "portal"


class BitrixGDSConnectionFormFactory(ConnectionFormFactory):
    def get_form_config(
        self,
        connector_settings: Optional[ConnectorSettingsBase],
        tenant: Optional[TenantDef],
    ) -> ConnectionForm:
        rc = RowConstructor(localizer=self._localizer)

        token_row = C.CustomizableRow(
            items=[
                C.LabelRowItem(text=self._localizer.translate(Translatable("label_token"))),
                C.InputRowItem(
                    name=CommonFieldName.token,
                    width="l",
                    control_props=C.InputRowItem.Props(type="password"),
                    default_value="" if self.mode == ConnectionFormMode.create else None,
                    fake_value=None if self.mode == ConnectionFormMode.create else "******",
                ),
            ]
        )

        portal_row = C.CustomizableRow(
            items=[
                C.LabelRowItem(text=self._localizer.translate(Translatable("label_portal"))),
                C.InputRowItem(name=BitrixGDSFieldName.portal),
            ]
        )

        rows: list[FormRow] = [
            portal_row,
            token_row,
        ]
        if self.mode == ConnectionFormMode.create:
            rows.append(rc.auto_create_dash_row())

        edit_api_schema = FormActionApiSchema(
            items=[
                FormFieldApiSchema(name=BitrixGDSFieldName.portal, required=True),
                FormFieldApiSchema(name=CommonFieldName.token, required=self.mode == ConnectionFormMode.create),
            ]
        )

        create_api_schema = FormActionApiSchema(
            items=[
                *edit_api_schema.items,
                *self._get_top_level_create_api_schema_items(),
            ]
        )

        check_api_schema = FormActionApiSchema(
            items=[
                *edit_api_schema.items,
                *self._get_top_level_check_api_schema_items(),
            ]
        )

        return ConnectionForm(
            title=BitrixGDSConnectionInfoProvider.get_title(self._localizer),
            template_name="bitrix24",
            rows=rows,
            api_schema=FormApiSchema(
                create=create_api_schema if self.mode == ConnectionFormMode.create else None,
                edit=edit_api_schema if self.mode == ConnectionFormMode.edit else None,
                check=check_api_schema,
            ),
        )
