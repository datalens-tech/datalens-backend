from enum import unique
import typing

import attr

from dl_api_commons.base_models import TenantDef
from dl_api_connector.form_config.models import rows as C
from dl_api_connector.form_config.models.api_schema import (
    FormActionApiSchema,
    FormApiSchema,
    FormFieldApiSchema,
    TFieldName,
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
from dl_configs.connectors_settings import ConnectorSettingsBase
from dl_constants.enums import RawSQLLevel
from dl_i18n.localizer_base import Localizer

from dl_connector_clickhouse.api.connection_info import ClickHouseConnectionInfoProvider
from dl_connector_clickhouse.api.i18n.localizer import Translatable
from dl_connector_clickhouse.core.clickhouse.settings import ClickHouseConnectorSettings


@unique
class ClickHouseFieldName(FormFieldName):
    readonly = "readonly"


@attr.s
class ClickHouseRowConstructor:
    _localizer: Localizer = attr.ib()

    def readonly_mode_row(self) -> C.CustomizableRow:
        return C.CustomizableRow(
            items=[
                C.LabelRowItem(
                    align="start",
                    text=self._localizer.translate(Translatable("field_readonly-mode")),
                    display_conditions={CommonFieldName.advanced_settings: "opened"},
                ),
                C.SelectRowItem(
                    name=ClickHouseFieldName.readonly,
                    width="s",
                    available_values=[
                        C.SelectOption(content="0", value="0"),
                        C.SelectOption(content="1", value="1"),
                        C.SelectOption(content="2", value="2"),
                    ],
                    default_value="2",
                    control_props=C.SelectRowItem.Props(show_search=False),
                    display_conditions={CommonFieldName.advanced_settings: "opened"},
                ),
            ]
        )


class ClickHouseConnectionFormFactory(ConnectionFormFactory):
    DEFAULT_PORT = "8443"

    def _get_implicit_form_fields(self) -> set[TFieldName]:
        return set()

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
            FormFieldApiSchema(name=ClickHouseFieldName.readonly),
        ]

    def _get_edit_api_schema(
        self,
        connector_settings: ConnectorSettingsBase | None,
    ) -> FormActionApiSchema:
        return FormActionApiSchema(
            items=[
                *self.get_common_api_schema_items(),
                FormFieldApiSchema(name=CommonFieldName.cache_ttl_sec, nullable=True),
                FormFieldApiSchema(name=CommonFieldName.raw_sql_level),
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

    def _get_check_api_schema(
        self,
        connector_settings: ConnectorSettingsBase | None,
    ) -> FormActionApiSchema:
        return FormActionApiSchema(
            items=[
                *self.get_common_api_schema_items(),
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
        return [rc.port_row(label_text=Translatable("field_click-house-port"), default_value=self.DEFAULT_PORT)]

    def _get_username_section(
        self,
        rc: RowConstructor,
        connector_settings: ConnectorSettingsBase | None,
    ) -> typing.Sequence[FormRow]:
        return [rc.username_row()]

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
        assert connector_settings is not None and isinstance(connector_settings, ClickHouseConnectorSettings)
        clickhouse_rc = ClickHouseRowConstructor(localizer=self._localizer)

        raw_sql_levels = [RawSQLLevel.subselect, RawSQLLevel.dashsql]
        if connector_settings.ENABLE_DATASOURCE_TEMPLATE:
            raw_sql_levels.append(RawSQLLevel.template)

        return [
            C.CacheTTLRow(name=CommonFieldName.cache_ttl_sec),
            rc.raw_sql_level_row_v2(raw_sql_levels=raw_sql_levels),
            rc.collapse_advanced_settings_row(),
            *rc.ssl_rows(
                enabled_name=CommonFieldName.secure,
                enabled_help_text=self._localizer.translate(Translatable("label_clickhouse-ssl-enabled-tooltip")),
                enabled_default_value=True,
            ),
            rc.data_export_forbidden_row(),
            clickhouse_rc.readonly_mode_row(),
        ]

    def get_form_config(
        self,
        connector_settings: ConnectorSettingsBase | None,
        tenant: TenantDef | None,
    ) -> ConnectionForm:
        rc = RowConstructor(localizer=self._localizer)

        edit_api_schema = self._get_edit_api_schema(connector_settings)
        create_api_schema = self._get_create_api_schema(connector_settings, edit_api_schema)
        check_api_schema = self._get_check_api_schema(connector_settings)

        return ConnectionForm(
            title=ClickHouseConnectionInfoProvider.get_title(self._localizer),
            rows=self._filter_nulls(
                [
                    *self._get_host_section(rc, connector_settings),
                    *self._get_port_section(rc, connector_settings),
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
