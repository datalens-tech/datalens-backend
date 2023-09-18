from __future__ import annotations

from typing import (
    Optional,
    Sequence,
)

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
from dl_api_connector.form_config.models.rows.base import FormRow
from dl_api_connector.form_config.models.shortcuts.rows import RowConstructor
from dl_configs.connectors_settings import ConnectorSettingsBase
from dl_connector_clickhouse.bi.connection_info import ClickHouseConnectionInfoProvider
from dl_connector_clickhouse.bi.i18n.localizer import Translatable


class ClickHouseConnectionFormFactory(ConnectionFormFactory):
    @staticmethod
    def get_common_api_schema_items() -> list[FormFieldApiSchema]:
        return [
            FormFieldApiSchema(name=CommonFieldName.host, required=True),
            FormFieldApiSchema(name=CommonFieldName.port, required=True),
            FormFieldApiSchema(name=CommonFieldName.username),
            FormFieldApiSchema(name=CommonFieldName.password),
            FormFieldApiSchema(name=CommonFieldName.secure),
            FormFieldApiSchema(name=CommonFieldName.ssl_ca),
            FormFieldApiSchema(name=CommonFieldName.data_export_forbidden),
        ]

    def _get_base_edit_api_schema(self) -> FormActionApiSchema:
        return FormActionApiSchema(
            items=[
                *self.get_common_api_schema_items(),
                FormFieldApiSchema(name=CommonFieldName.cache_ttl_sec, nullable=True),
                FormFieldApiSchema(name=CommonFieldName.raw_sql_level),
            ],
        )

    def _get_base_create_api_schema(self, edit_api_schema: FormActionApiSchema) -> FormActionApiSchema:
        return FormActionApiSchema(
            items=[
                *edit_api_schema.items,
                *self._get_top_level_create_api_schema_items(),
            ],
            conditions=edit_api_schema.conditions.copy(),
        )

    def _get_base_check_api_schema(self) -> FormActionApiSchema:
        return FormActionApiSchema(
            items=[
                *self.get_common_api_schema_items(),
                *self._get_top_level_check_api_schema_items(),
            ]
        )

    def _get_base_form_config(
        self,
        host_section: Sequence[FormRow],
        username_section: Sequence[FormRow],
        create_api_schema: FormActionApiSchema,
        edit_api_schema: FormActionApiSchema,
        check_api_schema: FormActionApiSchema,
        rc: RowConstructor,
    ) -> ConnectionForm:
        return ConnectionForm(
            title=ClickHouseConnectionInfoProvider.get_title(self._localizer),
            rows=self._filter_nulls(
                [
                    *host_section,
                    rc.port_row(label_text=Translatable("field_click-house-port"), default_value="8443"),
                    *username_section,
                    rc.password_row(mode=self.mode),
                    C.CacheTTLRow(name=CommonFieldName.cache_ttl_sec),
                    rc.raw_sql_level_row(),
                    rc.collapse_advanced_settings_row(),
                    *rc.ssl_rows(
                        enabled_name=CommonFieldName.secure,
                        enabled_help_text=self._localizer.translate(
                            Translatable("label_clickhouse-ssl-enabled-tooltip")
                        ),
                        enabled_default_value=True,
                    ),
                    rc.data_export_forbidden_row(),
                ]
            ),
            api_schema=FormApiSchema(
                create=create_api_schema if self.mode == ConnectionFormMode.create else None,
                edit=edit_api_schema if self.mode == ConnectionFormMode.edit else None,
                check=check_api_schema,
            ),
        )

    def get_form_config(
        self,
        connector_settings: Optional[ConnectorSettingsBase],
        tenant: Optional[TenantDef],
    ) -> ConnectionForm:
        rc = RowConstructor(localizer=self._localizer)

        host_section: list[FormRow] = [rc.host_row()]
        username_section: list[FormRow] = [rc.username_row()]

        edit_api_schema = self._get_base_edit_api_schema()
        create_api_schema = self._get_base_create_api_schema(edit_api_schema)
        check_api_schema = self._get_base_check_api_schema()

        return self._get_base_form_config(
            host_section=host_section,
            username_section=username_section,
            create_api_schema=create_api_schema,
            edit_api_schema=edit_api_schema,
            check_api_schema=check_api_schema,
            rc=rc,
        )
