from __future__ import annotations

from enum import unique
from typing import Optional

from bi_configs.connectors_settings import ConnectorsSettingsByType

from bi_api_commons.base_models import TenantDef

import bi_api_connector.form_config.models.rows as C
from bi_api_connector.form_config.models.api_schema import FormActionApiSchema, FormFieldApiSchema, FormApiSchema
from bi_api_connector.form_config.models.base import ConnectionFormFactory, ConnectionForm, ConnectionFormMode
from bi_api_connector.form_config.models.common import CommonFieldName, FormFieldName

from bi_connector_gsheets.bi.connection_info import GSheetsConnectionInfoProvider
from bi_connector_gsheets.bi.i18n.localizer import Translatable


@unique
class GSheetsFieldName(FormFieldName):
    url = 'url'


class GSheetsConnectionFormFactory(ConnectionFormFactory):
    def get_form_config(
            self,
            connectors_settings: Optional[ConnectorsSettingsByType],
            tenant: Optional[TenantDef],
    ) -> ConnectionForm:
        edit_api_schema = FormActionApiSchema(items=[
            FormFieldApiSchema(name=GSheetsFieldName.url, required=True),
            FormFieldApiSchema(name=CommonFieldName.cache_ttl_sec, nullable=True),
        ])

        create_api_schema = FormActionApiSchema(items=[
            *edit_api_schema.items,
            *self._get_top_level_create_api_schema_items(),
        ])

        check_api_schema = FormActionApiSchema(items=[
            FormFieldApiSchema(name=GSheetsFieldName.url, required=True),
            *self._get_top_level_check_api_schema_items(),
        ])

        return ConnectionForm(
            title=GSheetsConnectionInfoProvider.get_title(self._localizer),
            rows=[
                C.CustomizableRow(items=[
                    C.LabelRowItem(
                        text=self._localizer.translate(Translatable('field_google-sheets-link')),
                        help_text=self._localizer.translate(Translatable('label_gsheets-url-hint')),
                    ),
                    C.InputRowItem(name=GSheetsFieldName.url, width='l'),
                ]),
                C.CacheTTLRow(name=CommonFieldName.cache_ttl_sec),
            ],
            api_schema=FormApiSchema(
                create=create_api_schema if self.mode == ConnectionFormMode.create else None,
                edit=edit_api_schema if self.mode == ConnectionFormMode.edit else None,
                check=check_api_schema,
            ),
        )
