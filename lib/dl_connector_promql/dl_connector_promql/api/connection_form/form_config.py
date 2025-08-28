from __future__ import annotations

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
    FormUIOverride,
)
from dl_api_connector.form_config.models.common import CommonFieldName
import dl_api_connector.form_config.models.rows as C
from dl_api_connector.form_config.models.shortcuts.rows import RowConstructor
from dl_configs.connectors_settings import ConnectorSettingsBase

from dl_connector_promql.api.connection_info import PromQLConnectionInfoProvider


class PromQLConnectionFormFactory(ConnectionFormFactory):
    def get_form_config(
        self,
        connector_settings: Optional[ConnectorSettingsBase],
        tenant: Optional[TenantDef],
    ) -> ConnectionForm:
        rc = RowConstructor(localizer=self._localizer)

        edit_api_schema = FormActionApiSchema(
            items=[
                FormFieldApiSchema(name=CommonFieldName.host, required=True),
                FormFieldApiSchema(name=CommonFieldName.port, required=True),
                FormFieldApiSchema(name=CommonFieldName.path),
                FormFieldApiSchema(name=CommonFieldName.username),
                FormFieldApiSchema(name=CommonFieldName.password),
                FormFieldApiSchema(name=CommonFieldName.cache_ttl_sec, nullable=True),
                FormFieldApiSchema(name=CommonFieldName.secure, type="boolean"),
                FormFieldApiSchema(name=CommonFieldName.data_export_forbidden),
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
                FormFieldApiSchema(name=CommonFieldName.host, required=True),
                FormFieldApiSchema(name=CommonFieldName.port, required=True),
                FormFieldApiSchema(name=CommonFieldName.path),
                FormFieldApiSchema(name=CommonFieldName.username),
                FormFieldApiSchema(name=CommonFieldName.password),
                FormFieldApiSchema(name=CommonFieldName.secure, type="boolean"),
                *self._get_top_level_check_api_schema_items(),
            ]
        )

        form_params = self._get_form_params()

        return ConnectionForm(
            title=PromQLConnectionInfoProvider.get_title(self._localizer),
            rows=[
                rc.host_row(),
                rc.port_row(),
                rc.path_row(),
                rc.username_row(),
                rc.password_row(self.mode),
                C.CacheTTLRow(name=CommonFieldName.cache_ttl_sec),
                C.CustomizableRow(
                    items=[
                        C.CheckboxRowItem(name=CommonFieldName.secure, text="HTTPS", default_value=True),
                    ]
                ),
                rc.collapse_advanced_settings_row(),
                rc.data_export_forbidden_row(
                    conn_id=form_params.conn_id,
                    exports_history_url_path=form_params.exports_history_url_path,
                    mode=self.mode,
                ),
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
