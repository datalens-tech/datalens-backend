from __future__ import annotations

from typing import Optional

from bi_connector_mdb_base.bi.form_config.models.common import MDBFieldName
from bi_connector_mdb_base.bi.form_config.models.rows.prepared import components as mdb_components
from bi_configs.connectors_settings import ConnectorSettingsBase

from bi_api_commons.base_models import TenantDef

from bi_api_connector.form_config.models.api_schema import FormActionApiSchema, FormFieldApiSchema, FormApiSchema
from bi_api_connector.form_config.models.base import (
    ConnectionFormFactory,
    ConnectionForm,
    ConnectionFormMode,
    FormUIOverride,
)

from bi_connector_monitoring.bi.connection_info import MonitoringConnectionInfoProvider


class MonitoringConnectionFormFactory(ConnectionFormFactory):
    def get_form_config(
            self,
            connector_settings: Optional[ConnectorSettingsBase],
            tenant: Optional[TenantDef],
    ) -> ConnectionForm:

        edit_api_schema = FormActionApiSchema(items=[
            FormFieldApiSchema(name=MDBFieldName.folder_id, required=True),
            FormFieldApiSchema(name=MDBFieldName.service_account_id, required=True),
        ])

        create_api_schema = FormActionApiSchema(items=[
            *edit_api_schema.items,
            *self._get_top_level_create_api_schema_items(),
        ])

        return ConnectionForm(
            title=MonitoringConnectionInfoProvider.get_title(self._localizer),
            rows=[
                mdb_components.CloudTreeSelectRow(name=MDBFieldName.folder_id),
                mdb_components.ServiceAccountRow(name=MDBFieldName.service_account_id),
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
