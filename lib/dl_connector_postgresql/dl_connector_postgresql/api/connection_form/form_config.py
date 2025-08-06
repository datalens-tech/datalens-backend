from enum import unique
import typing

import attr

from dl_api_commons.base_models import (
    FormConfigParams,
    TenantDef,
)
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
    TFieldName,
)
from dl_api_connector.form_config.models.rows.base import FormRow
from dl_api_connector.form_config.models.shortcuts.rows import RowConstructor
from dl_configs.connectors_settings import ConnectorSettingsBase
from dl_constants.enums import RawSQLLevel
from dl_i18n.localizer_base import Localizer

from dl_connector_postgresql.api.connection_info import PostgreSQLConnectionInfoProvider
from dl_connector_postgresql.api.i18n.localizer import Translatable
from dl_connector_postgresql.core.postgresql.settings import PostgreSQLConnectorSettings
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
    DEFAULT_PORT = "6432"

    def _get_implicit_form_fields(self) -> set[TFieldName]:
        return set()

    def _get_edit_api_schema(self, connector_settings: ConnectorSettingsBase | None) -> FormActionApiSchema:
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

    def _get_create_api_schema(
        self,
        connector_settings: ConnectorSettingsBase | None,
        edit_api_schema: FormActionApiSchema,
    ) -> FormActionApiSchema:
        return FormActionApiSchema(
            items=[
                *edit_api_schema.items,
                *self._get_top_level_create_api_schema_items(),
            ],
            conditions=edit_api_schema.conditions.copy(),
        )

    def _get_check_api_schema(self, connector_settings: ConnectorSettingsBase | None) -> FormActionApiSchema:
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

    def _get_host_section(
        self,
        rc: RowConstructor,
        connector_settings: ConnectorSettingsBase | None,
    ) -> typing.Sequence[FormRow]:
        return [rc.host_row()]

    def _get_port_section(
        self,
        rc: RowConstructor,
        connector_settings: ConnectorSettingsBase | None,
    ) -> typing.Sequence[FormRow]:
        return [rc.port_row(default_value=self.DEFAULT_PORT)]

    def _get_username_section(
        self,
        rc: RowConstructor,
        connector_settings: ConnectorSettingsBase | None,
    ) -> typing.Sequence[FormRow]:
        return [rc.username_row()]

    def _get_db_name_section(
        self,
        rc: RowConstructor,
        connector_settings: ConnectorSettingsBase | None,
    ) -> typing.Sequence[FormRow]:
        return [rc.db_name_row()]

    def _get_password_section(
        self,
        rc: RowConstructor,
        connector_settings: ConnectorSettingsBase | None,
    ) -> typing.Sequence[FormRow]:
        return [rc.password_row(mode=self.mode)]

    def _get_common_section(
        self,
        rc: RowConstructor,
        connector_settings: ConnectorSettingsBase | None,
    ) -> typing.Sequence[FormRow]:
        assert connector_settings is not None and isinstance(connector_settings, PostgreSQLConnectorSettings)
        postgres_rc = PostgresRowConstructor(localizer=self._localizer)

        raw_sql_levels = [RawSQLLevel.subselect, RawSQLLevel.dashsql]
        if connector_settings.ENABLE_DATASOURCE_TEMPLATE:
            raw_sql_levels.append(RawSQLLevel.template)

        return [
            C.CacheTTLRow(name=CommonFieldName.cache_ttl_sec),
            rc.raw_sql_level_row_v2(raw_sql_levels=raw_sql_levels),
            rc.collapse_advanced_settings_row(),
            postgres_rc.enforce_collate_row(),
            *rc.ssl_rows(
                enabled_name=CommonFieldName.ssl_enable,
                enabled_help_text=self._localizer.translate(Translatable("label_postgres-ssl-enabled-tooltip")),
                enabled_default_value=False,
            ),
            rc.data_export_forbidden_row(),
        ]

    def get_form_config(
        self,
        connector_settings: ConnectorSettingsBase | None,
        tenant: TenantDef | None,
        params: FormConfigParams | None = None,
    ) -> ConnectionForm:
        rc = RowConstructor(localizer=self._localizer)

        edit_api_schema = self._get_edit_api_schema(connector_settings)
        check_api_schema = self._get_check_api_schema(connector_settings)
        create_api_schema = self._get_create_api_schema(connector_settings, edit_api_schema)

        return ConnectionForm(
            title=PostgreSQLConnectionInfoProvider.get_title(self._localizer),
            rows=self._filter_nulls(
                [
                    *self._get_host_section(rc, connector_settings),
                    *self._get_port_section(rc, connector_settings),
                    *self._get_db_name_section(rc, connector_settings),
                    *self._get_username_section(rc, connector_settings),
                    *self._get_password_section(rc, connector_settings),
                    *self._get_common_section(rc, connector_settings),
                ]
            ),
            api_schema=FormApiSchema(
                create=create_api_schema if self.mode == ConnectionFormMode.create else None,
                edit=edit_api_schema if self.mode == ConnectionFormMode.edit else None,
                check=check_api_schema,
            ),
            implicit_form_fields=self._get_implicit_form_fields(),
        )
