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
from dl_connector_greenplum.bi.connection_info import GreenplumConnectionInfoProvider


class GreenplumConnectionFormFactory(ConnectionFormFactory):
    def _get_base_edit_api_schema(self) -> FormActionApiSchema:
        return FormActionApiSchema(
            items=[
                FormFieldApiSchema(name=CommonFieldName.host, required=True),
                FormFieldApiSchema(name=CommonFieldName.port, required=True),
                FormFieldApiSchema(name=CommonFieldName.username, required=True),
                FormFieldApiSchema(name=CommonFieldName.db_name, required=True),
                FormFieldApiSchema(
                    name=CommonFieldName.password,
                    required=self.mode == ConnectionFormMode.create,
                ),
                FormFieldApiSchema(name=CommonFieldName.cache_ttl_sec, nullable=True),
                FormFieldApiSchema(name=CommonFieldName.raw_sql_level),
                FormFieldApiSchema(name=CommonFieldName.data_export_forbidden),
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
                FormFieldApiSchema(name=CommonFieldName.host, required=True),
                FormFieldApiSchema(name=CommonFieldName.port, required=True),
                FormFieldApiSchema(name=CommonFieldName.db_name, required=True),
                FormFieldApiSchema(name=CommonFieldName.username, required=True),
                FormFieldApiSchema(
                    name=CommonFieldName.password,
                    required=self.mode == ConnectionFormMode.create,
                ),
                *self._get_top_level_check_api_schema_items(),
            ]
        )

    def _get_base_form_config(
        self,
        host_section: Sequence[FormRow],
        username_section: Sequence[FormRow],
        db_name_section: Sequence[FormRow],
        create_api_schema: FormActionApiSchema,
        edit_api_schema: FormActionApiSchema,
        check_api_schema: FormActionApiSchema,
        rc: RowConstructor,
    ) -> ConnectionForm:
        return ConnectionForm(
            title=GreenplumConnectionInfoProvider.get_title(self._localizer),
            rows=self._filter_nulls(
                [
                    *host_section,
                    rc.port_row(default_value="5432"),
                    *db_name_section,
                    *username_section,
                    rc.password_row(mode=self.mode),
                    C.CacheTTLRow(name=CommonFieldName.cache_ttl_sec),
                    rc.raw_sql_level_row(),
                    rc.collapse_advanced_settings_row(),
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
        db_name_section: list[FormRow] = [rc.db_name_row()]

        edit_api_schema = self._get_base_edit_api_schema()
        create_api_schema = self._get_base_create_api_schema(edit_api_schema)
        check_api_schema = self._get_base_check_api_schema()

        return self._get_base_form_config(
            host_section=host_section,
            username_section=username_section,
            db_name_section=db_name_section,
            create_api_schema=create_api_schema,
            edit_api_schema=edit_api_schema,
            check_api_schema=check_api_schema,
            rc=rc,
        )
