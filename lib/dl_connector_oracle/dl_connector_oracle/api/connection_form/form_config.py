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
from dl_api_connector.form_config.models.shortcuts.rows import RowConstructor
from dl_configs.connectors_settings import ConnectorSettingsBase

from dl_connector_oracle.api.connection_info import OracleConnectionInfoProvider
from dl_connector_oracle.api.i18n.localizer import Translatable
from dl_connector_oracle.core.constants import OracleDbNameType


@unique
class OracleFieldName(FormFieldName):
    db_connect_method = "db_connect_method"


class OracleConnectionFormFactory(ConnectionFormFactory):
    def get_form_config(
        self,
        connector_settings: Optional[ConnectorSettingsBase],
        tenant: Optional[TenantDef],
    ) -> ConnectionForm:
        rc = RowConstructor(self._localizer)

        common_api_schema_items: list[FormFieldApiSchema] = [
            FormFieldApiSchema(name=CommonFieldName.host, required=True),
            FormFieldApiSchema(name=CommonFieldName.port, required=True),
            FormFieldApiSchema(name=OracleFieldName.db_connect_method, required=True),
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

        db_name_row = C.CustomizableRow(
            items=[
                *rc.db_name_row().items,
                C.RadioButtonRowItem(
                    name=OracleFieldName.db_connect_method,
                    options=[
                        C.SelectableOption(
                            text=self._localizer.translate(Translatable("value_db-connect-method-service-name")),
                            value=OracleDbNameType.service_name.value,
                        ),
                        C.SelectableOption(
                            text=self._localizer.translate(Translatable("value_db-connect-method-sid")),
                            value=OracleDbNameType.sid.value,
                        ),
                    ],
                    default_value=OracleDbNameType.service_name.value,
                ),
            ]
        )

        return ConnectionForm(
            title=OracleConnectionInfoProvider.get_title(self._localizer),
            rows=[
                rc.host_row(),
                rc.port_row(default_value="1521"),
                db_name_row,
                rc.username_row(),
                rc.password_row(self.mode),
                C.CacheTTLRow(name=CommonFieldName.cache_ttl_sec),
                rc.raw_sql_level_row(),
                rc.collapse_advanced_settings_row(),
                rc.data_export_forbidden_row(),
            ],
            api_schema=FormApiSchema(
                create=create_api_schema if self.mode == ConnectionFormMode.create else None,
                edit=edit_api_schema if self.mode == ConnectionFormMode.edit else None,
                check=check_api_schema,
            ),
        )
