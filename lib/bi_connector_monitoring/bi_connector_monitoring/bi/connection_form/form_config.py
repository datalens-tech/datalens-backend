from __future__ import annotations

from typing import Optional

from bi_configs.connectors_settings import ConnectorSettingsBase

from bi_api_commons.base_models import TenantDef

import bi_api_connector.form_config.models.rows as C
from bi_api_connector.form_config.models.api_schema import FormActionApiSchema, FormFieldApiSchema, FormApiSchema
from bi_api_connector.form_config.models.base import (
    ConnectionFormFactory,
    ConnectionForm,
    ConnectionFormMode,
    FormUIOverride,
)
from bi_api_connector.form_config.models.common import CommonFieldName

from bi_connector_monitoring.bi.connection_info import MonitoringConnectionInfoProvider


class MonitoringConnectionFormFactory(ConnectionFormFactory):
    def get_form_config(
            self,
            connector_settings: Optional[ConnectorSettingsBase],
            tenant: Optional[TenantDef],
    ) -> ConnectionForm:

        edit_api_schema = FormActionApiSchema(items=[
            FormFieldApiSchema(name=CommonFieldName.folder_id, required=True),
            FormFieldApiSchema(name=CommonFieldName.service_account_id, required=True),
        ])

        create_api_schema = FormActionApiSchema(items=[
            *edit_api_schema.items,
            *self._get_top_level_create_api_schema_items(),
        ])

        return ConnectionForm(
            title=MonitoringConnectionInfoProvider.get_title(self._localizer),
            rows=[
                C.CloudTreeSelectRow(name=CommonFieldName.folder_id),
                C.ServiceAccountRow(name=CommonFieldName.service_account_id),
            ],
            api_schema=FormApiSchema(
                create=create_api_schema if self.mode == ConnectionFormMode.create else None,
                edit=edit_api_schema if self.mode == ConnectionFormMode.edit else None,
            ),
            form_ui_override=FormUIOverride(
                show_create_dataset_btn=False,
                show_create_ql_chart_btn=True,
            ),
        )
