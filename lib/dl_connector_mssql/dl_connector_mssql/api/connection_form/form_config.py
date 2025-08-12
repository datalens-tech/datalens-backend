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
)
from dl_api_connector.form_config.models.common import CommonFieldName
import dl_api_connector.form_config.models.rows as C
from dl_api_connector.form_config.models.shortcuts.rows import RowConstructor
from dl_configs.connectors_settings import ConnectorSettingsBase
from dl_constants.enums import RawSQLLevel

from dl_connector_mssql.api.connection_info import MSSQLConnectionInfoProvider
from dl_connector_mssql.core.settings import MSSQLConnectorSettings


class MSSQLConnectionFormFactory(ConnectionFormFactory):
    def get_form_config(
        self,
        connector_settings: Optional[ConnectorSettingsBase],
        tenant: Optional[TenantDef],
    ) -> ConnectionForm:
        assert connector_settings is not None and isinstance(connector_settings, MSSQLConnectorSettings)

        rc = RowConstructor(localizer=self._localizer)

        common_api_schema_items: list[FormFieldApiSchema] = [
            FormFieldApiSchema(name=CommonFieldName.host, required=True),
            FormFieldApiSchema(name=CommonFieldName.port, required=True),
            FormFieldApiSchema(name=CommonFieldName.db_name, required=True),
            FormFieldApiSchema(name=CommonFieldName.username, required=True),
            FormFieldApiSchema(name=CommonFieldName.password, required=self.mode == ConnectionFormMode.create),
        ]

        edit_api_schema = FormActionApiSchema(
            items=[
                *common_api_schema_items,
                FormFieldApiSchema(name=CommonFieldName.cache_ttl_sec, nullable=True),
                FormFieldApiSchema(name=CommonFieldName.raw_sql_level),
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
                *common_api_schema_items,
                *self._get_top_level_check_api_schema_items(),
            ]
        )

        raw_sql_levels = [RawSQLLevel.subselect, RawSQLLevel.dashsql]
        if connector_settings.ENABLE_DATASOURCE_TEMPLATE:
            raw_sql_levels.append(RawSQLLevel.template)

        return ConnectionForm(
            title=MSSQLConnectionInfoProvider.get_title(self._localizer),
            rows=[
                rc.host_row(),
                rc.port_row(default_value="1433"),
                rc.db_name_row(),
                rc.username_row(),
                rc.password_row(self.mode),
                C.CacheTTLRow(name=CommonFieldName.cache_ttl_sec),
                rc.raw_sql_level_row_v2(raw_sql_levels=raw_sql_levels),
                rc.collapse_advanced_settings_row(),
                rc.data_export_forbidden_row(
                    conn_id=self._get_form_params().conn_id,
                    exports_history_url_path=self._get_form_params().exports_history_url_path,
                    mode=self.mode,
                ),
            ],
            api_schema=FormApiSchema(
                create=create_api_schema if self.mode == ConnectionFormMode.create else None,
                edit=edit_api_schema if self.mode == ConnectionFormMode.edit else None,
                check=check_api_schema,
            ),
        )
