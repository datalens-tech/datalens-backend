from enum import unique

import attr

from dl_api_commons.base_models import TenantDef
from dl_api_connector.form_config.models.api_schema import (
    FormActionApiSchema,
    FormApiSchema,
    FormFieldApiAction,
    FormFieldApiActionCondition,
    FormFieldApiSchema,
    FormFieldConditionalApiAction,
    FormFieldSelector,
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
import dl_api_connector.form_config.models.rows as C
from dl_api_connector.form_config.models.rows.base import TDisplayConditions
from dl_api_connector.form_config.models.shortcuts.rows import RowConstructor
from dl_configs.connectors_settings import ConnectorSettingsBase
from dl_constants.enums import RawSQLLevel
from dl_i18n.localizer_base import Localizer

from dl_connector_trino.api.connection_info import TrinoConnectionInfoProvider
from dl_connector_trino.api.i18n.localizer import Translatable
from dl_connector_trino.core.constants import (
    ListingSources,
    TrinoAuthType,
)


@unique
class TrinoFormFieldName(FormFieldName):
    auth_type = "auth_type"
    jwt = "jwt"
    listing_sources = "listing_sources"


@attr.s
class TrinoRowConstructor(RowConstructor):
    _localizer: Localizer = attr.ib()

    def auth_type_row(self) -> C.CustomizableRow:
        return C.CustomizableRow(
            items=[
                C.LabelRowItem(
                    text=self._localizer.translate(Translatable("field_auth-type")),
                ),
                C.RadioButtonRowItem(
                    name=TrinoFormFieldName.auth_type,
                    options=[
                        C.SelectableOption(
                            text=self._localizer.translate(Translatable("value_auth-type-none")),
                            value=TrinoAuthType.none.value,
                        ),
                        C.SelectableOption(
                            text=self._localizer.translate(Translatable("value_auth-type-password")),
                            value=TrinoAuthType.password.value,
                        ),
                        C.SelectableOption(
                            text=self._localizer.translate(Translatable("value_auth-type-jwt")),
                            value=TrinoAuthType.jwt.value,
                        ),
                    ],
                    default_value=TrinoAuthType.password.value,
                ),
            ]
        )

    def password_row(
        self,
        mode: ConnectionFormMode,
        display_conditions: TDisplayConditions | None = None,
    ) -> C.CustomizableRow:
        display_conditions = {TrinoFormFieldName.auth_type: TrinoAuthType.password.value}
        label_text = self._localizer.translate(Translatable("field_password"))
        return C.CustomizableRow(
            items=[
                C.LabelRowItem(text=label_text, display_conditions=display_conditions),
                C.InputRowItem(
                    name=CommonFieldName.password,
                    width="m",
                    default_value="" if mode == ConnectionFormMode.create else None,
                    fake_value="******" if mode == ConnectionFormMode.edit else None,
                    control_props=C.InputRowItem.Props(type="password"),
                    display_conditions=display_conditions,
                ),
            ]
        )

    def jwt_row(
        self,
        mode: ConnectionFormMode,
        display_conditions: TDisplayConditions | None = None,
    ) -> C.CustomizableRow:
        display_conditions = {TrinoFormFieldName.auth_type: TrinoAuthType.jwt.value}
        label_text = self._localizer.translate(Translatable("field_jwt"))
        return C.CustomizableRow(
            items=[
                C.LabelRowItem(text=label_text, display_conditions=display_conditions),
                C.InputRowItem(
                    name=TrinoFormFieldName.jwt,
                    width="l",
                    default_value="" if mode == ConnectionFormMode.create else None,
                    fake_value="******" if mode == ConnectionFormMode.edit else None,
                    control_props=C.InputRowItem.Props(type="password"),
                    display_conditions=display_conditions,
                ),
            ]
        )

    def cache_ttl_row(self) -> C.CacheTTLRow:
        return C.CacheTTLRow(name=CommonFieldName.cache_ttl_sec)

    def trino_ssl_rows(
        self,
        display_conditions: TDisplayConditions | None = None,
    ) -> list[C.CustomizableRow]:
        return list(
            self.ssl_rows(
                enabled_name=CommonFieldName.ssl_enable,
                enabled_help_text=self._localizer.translate(Translatable("label_trino-ssl-enabled-tooltip")),
                enabled_default_value=True,
                display_conditions=display_conditions,
            )
        )

    def listing_sources_row(self) -> C.CustomizableRow:
        return C.CustomizableRow(
            items=[
                C.LabelRowItem(
                    text=self._localizer.translate(Translatable("field_listing-sources")),
                    display_conditions={CommonFieldName.advanced_settings: "opened"},
                    help_text=self._localizer.translate(Translatable("label_listing-sources-tooltip")),
                ),
                C.RadioButtonRowItem(
                    name=TrinoFormFieldName.listing_sources,
                    options=[
                        C.SelectableOption(
                            text=self._localizer.translate(Translatable("value_listing-sources-off")),
                            value=ListingSources.off.value,
                        ),
                        C.SelectableOption(
                            text=self._localizer.translate(Translatable("value_listing-sources-on")),
                            value=ListingSources.on.value,
                        ),
                    ],
                    default_value=ListingSources.on.value,
                    display_conditions={CommonFieldName.advanced_settings: "opened"},
                ),
            ]
        )


class TrinoConnectionFormFactory(ConnectionFormFactory):
    DEFAULT_HTTPS_PORT = "8443"

    def _get_implicit_form_fields(self) -> set[TFieldName]:
        return set()

    def _get_schema_items(self) -> list[FormFieldApiSchema]:
        return [
            FormFieldApiSchema(name=TrinoFormFieldName.auth_type, required=True),
            FormFieldApiSchema(name=CommonFieldName.host, required=True),
            FormFieldApiSchema(name=CommonFieldName.port, required=True),
            FormFieldApiSchema(name=CommonFieldName.username, required=True),
            FormFieldApiSchema(
                name=CommonFieldName.password,
                required=self.mode == ConnectionFormMode.create,
                default_action=FormFieldApiAction.skip,
            ),
            FormFieldApiSchema(
                name=TrinoFormFieldName.jwt,
                required=self.mode == ConnectionFormMode.create,
                default_action=FormFieldApiAction.skip,
            ),
            FormFieldApiSchema(name=CommonFieldName.cache_ttl_sec, nullable=True),
            FormFieldApiSchema(name=CommonFieldName.raw_sql_level),
            FormFieldApiSchema(name=CommonFieldName.data_export_forbidden),
            FormFieldApiSchema(name=TrinoFormFieldName.listing_sources),
            FormFieldApiSchema(name=CommonFieldName.ssl_enable),
            FormFieldApiSchema(name=CommonFieldName.ssl_ca),
        ]

    def _get_schema_conditions(self) -> list[FormFieldApiActionCondition]:
        return [
            FormFieldApiActionCondition(
                when=FormFieldSelector(name=TrinoFormFieldName.auth_type),
                equals=TrinoAuthType.password.value,
                then=[
                    FormFieldConditionalApiAction(
                        selector=FormFieldSelector(name=CommonFieldName.password),
                        action=FormFieldApiAction.include,
                    ),
                ],
            ),
            FormFieldApiActionCondition(
                when=FormFieldSelector(name=TrinoFormFieldName.auth_type),
                equals=TrinoAuthType.jwt.value,
                then=[
                    FormFieldConditionalApiAction(
                        selector=FormFieldSelector(name=TrinoFormFieldName.jwt),
                        action=FormFieldApiAction.include,
                    ),
                ],
            ),
        ]

    def _get_edit_api_schema(
        self,
        connector_settings: ConnectorSettingsBase | None,
    ) -> FormActionApiSchema:
        return FormActionApiSchema(
            items=self._get_schema_items(),
            conditions=self._get_schema_conditions(),
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
                *self._get_schema_items(),
                *self._get_top_level_check_api_schema_items(),
            ],
            conditions=self._get_schema_conditions(),
        )

    def get_form_config(
        self,
        connector_settings: ConnectorSettingsBase | None,
        tenant: TenantDef | None,
    ) -> ConnectionForm:
        rc = TrinoRowConstructor(localizer=self._localizer)

        edit_api_schema = self._get_edit_api_schema(connector_settings)
        create_api_schema = self._get_create_api_schema(connector_settings, edit_api_schema)
        check_api_schema = self._get_check_api_schema(connector_settings)

        return ConnectionForm(
            title=TrinoConnectionInfoProvider.get_title(self._localizer),
            rows=self._filter_nulls(
                [
                    rc.auth_type_row(),
                    rc.host_row(),
                    rc.port_row(default_value=self.DEFAULT_HTTPS_PORT),
                    rc.username_row(),
                    rc.password_row(mode=self.mode),
                    rc.jwt_row(mode=self.mode),
                    rc.cache_ttl_row(),
                    rc.raw_sql_level_row_v2(raw_sql_levels=[RawSQLLevel.subselect, RawSQLLevel.dashsql]),
                    rc.collapse_advanced_settings_row(),
                    *rc.trino_ssl_rows(),
                    rc.data_export_forbidden_row(),
                    rc.listing_sources_row(),
                ]
            ),
            api_schema=FormApiSchema(
                create=create_api_schema if self.mode == ConnectionFormMode.create else None,
                edit=edit_api_schema if self.mode == ConnectionFormMode.edit else None,
                check=check_api_schema,
            ),
            implicit_form_fields=self._get_implicit_form_fields(),
        )
