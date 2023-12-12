from __future__ import annotations

from enum import unique
from typing import (
    Optional,
    Sequence,
)

import attr

from dl_api_commons.base_models import TenantDef
from dl_api_connector.form_config.models import rows as C
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
from dl_api_connector.form_config.models.rows.base import FormRow
from dl_api_connector.form_config.models.shortcuts.rows import RowConstructor
from dl_api_connector.i18n.localizer import Translatable
from dl_configs.connectors_settings import ConnectorSettingsBase
from dl_i18n.localizer_base import Localizer

from dl_connector_postgresql.api.connection_info import PostgreSQLConnectionInfoProvider
from dl_connector_postgresql.api.i18n.localizer import Translatable
from dl_connector_postgresql.core.postgresql_base.constants import PGEnforceCollateMode


@unique
class PostgreSQLFieldName(FormFieldName):
    enforce_collate = "enforce_collate"


@attr.s
class PostgresRowConstructor:
    _localizer: Localizer = attr.ib()

    def enforce_collate_row(self) -> C.CustomizableRow:
        return C.CustomizableRow(
            items=[
                C.LabelRowItem(
                    text=self._localizer.translate(Translatable("field_enforce-collate")),
                    display_conditions={CommonFieldName.advanced_settings: "opened"},
                ),
                C.RadioButtonRowItem(
                    name=PostgreSQLFieldName.enforce_collate,
                    options=[
                        C.SelectableOption(
                            text=self._localizer.translate(Translatable("value_enforce-collate-auto")),
                            value=PGEnforceCollateMode.auto.value,
                        ),
                        C.SelectableOption(
                            text=self._localizer.translate(Translatable("value_enforce-collate-off")),
                            value=PGEnforceCollateMode.off.value,
                        ),
                        C.SelectableOption(
                            text=self._localizer.translate(Translatable("value_enforce-collate-on")),
                            value=PGEnforceCollateMode.on.value,
                        ),
                    ],
                    default_value=PGEnforceCollateMode.auto.value,
                    display_conditions={CommonFieldName.advanced_settings: "opened"},
                ),
            ]
        )


class PostgreSQLConnectionFormFactory(ConnectionFormFactory):
    def _get_base_edit_api_schema(self) -> FormActionApiSchema:
        return FormActionApiSchema(
            items=[
                FormFieldApiSchema(name=CommonFieldName.host, required=True),
                FormFieldApiSchema(name=CommonFieldName.port, required=True),
                FormFieldApiSchema(name=CommonFieldName.username, required=True),
                FormFieldApiSchema(name=CommonFieldName.db_name, required=True),
                FormFieldApiSchema(name=CommonFieldName.password, required=self.mode == ConnectionFormMode.create),
                FormFieldApiSchema(name=CommonFieldName.cache_ttl_sec, nullable=True),
                FormFieldApiSchema(name=CommonFieldName.raw_sql_level),
                FormFieldApiSchema(name=PostgreSQLFieldName.enforce_collate),
                FormFieldApiSchema(name=CommonFieldName.ssl_enable),
                FormFieldApiSchema(name=CommonFieldName.ssl_ca),
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
                FormFieldApiSchema(name=CommonFieldName.password, required=self.mode == ConnectionFormMode.create),
                FormFieldApiSchema(name=CommonFieldName.ssl_enable),
                FormFieldApiSchema(name=CommonFieldName.ssl_ca),
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
        postgres_rc: PostgresRowConstructor,
    ) -> ConnectionForm:
        return ConnectionForm(
            title=PostgreSQLConnectionInfoProvider.get_title(self._localizer),
            rows=self._filter_nulls(
                [
                    *host_section,
                    rc.port_row(default_value="6432"),
                    *db_name_section,
                    *username_section,
                    rc.password_row(mode=self.mode),
                    C.CacheTTLRow(name=CommonFieldName.cache_ttl_sec),
                    rc.raw_sql_level_row(),
                    rc.collapse_advanced_settings_row(),
                    postgres_rc.enforce_collate_row(),
                    *rc.ssl_rows(
                        enabled_name=CommonFieldName.ssl_enable,
                        enabled_help_text=self._localizer.translate(Translatable("label_postgres-ssl-enabled-tooltip")),
                        enabled_default_value=False,
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
        postgres_rc = PostgresRowConstructor(localizer=self._localizer)

        host_section = [rc.host_row()]
        username_section = [rc.username_row()]
        db_name_section = [rc.db_name_row()]

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
            postgres_rc=postgres_rc,
        )
