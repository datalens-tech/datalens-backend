from __future__ import annotations

from typing import Optional

from bi_configs.connectors_settings import ConnectorsSettingsByType

from bi_api_commons.base_models import TenantDef

import bi_api_connector.form_config.models.rows as C
from bi_api_connector.form_config.models.shortcuts.rows import RowConstructor
from bi_api_connector.form_config.models.api_schema import FormActionApiSchema, FormFieldApiSchema, FormApiSchema
from bi_api_connector.form_config.models.base import (
    ConnectionFormFactory,
    ConnectionForm,
    ConnectionFormMode,
    FormUIOverride,
)
from bi_api_connector.form_config.models.common import CommonFieldName

from bi_connector_promql.bi.connection_info import PromQLConnectionInfoProvider


class PromQLConnectionFormFactory(ConnectionFormFactory):
    def get_form_config(
            self,
            connectors_settings: Optional[ConnectorsSettingsByType],
            tenant: Optional[TenantDef],
    ) -> ConnectionForm:
        rc = RowConstructor(localizer=self._localizer)

        edit_api_schema = FormActionApiSchema(items=[
            FormFieldApiSchema(name=CommonFieldName.host, required=True),
            FormFieldApiSchema(name=CommonFieldName.port, required=True),
            FormFieldApiSchema(name=CommonFieldName.username),
            FormFieldApiSchema(name=CommonFieldName.password),
            FormFieldApiSchema(name=CommonFieldName.cache_ttl_sec, nullable=True),
            FormFieldApiSchema(name=CommonFieldName.secure, type='boolean'),
        ])

        create_api_schema = FormActionApiSchema(items=[
            *edit_api_schema.items,
            *self._get_top_level_create_api_schema_items(),
        ])

        check_api_schema = FormActionApiSchema(items=[
            FormFieldApiSchema(name=CommonFieldName.host, required=True),
            FormFieldApiSchema(name=CommonFieldName.port, required=True),
            FormFieldApiSchema(name=CommonFieldName.username),
            FormFieldApiSchema(name=CommonFieldName.password),
            FormFieldApiSchema(name=CommonFieldName.secure, type='boolean'),
            *self._get_top_level_check_api_schema_items(),
        ])

        return ConnectionForm(
            title=PromQLConnectionInfoProvider.get_title(self._localizer),
            rows=[
                rc.host_row(),
                rc.port_row(),
                rc.username_row(),
                rc.password_row(self.mode),
                C.CacheTTLRow(name=CommonFieldName.cache_ttl_sec),
                C.CustomizableRow(items=[
                    C.CheckboxRowItem(name=CommonFieldName.secure, text='HTTPS', default_value=True),
                ])
            ],
            api_schema=FormApiSchema(
                create=create_api_schema if self.mode == ConnectionFormMode.create else None,
                edit=edit_api_schema if self.mode == ConnectionFormMode.edit else None,
                check=check_api_schema,
            ),
            form_ui_override=FormUIOverride(
                show_create_dataset_btn=False,
                show_create_ql_chart_btn=True,
            ),
        )
