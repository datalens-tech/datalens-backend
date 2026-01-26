from __future__ import annotations

from enum import unique
from typing import Optional

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
)
from dl_api_connector.form_config.models.base import (
    ConnectionForm,
    ConnectionFormFactory,
    ConnectionFormMode,
    FormUIOverride,
)
from dl_api_connector.form_config.models.common import (
    CommonFieldName,
    FormFieldName,
)
import dl_api_connector.form_config.models.rows as C
from dl_api_connector.form_config.models.rows.base import TDisplayConditions
from dl_api_connector.form_config.models.shortcuts.rows import RowConstructor
from dl_core.connectors.settings.base import ConnectorSettings
from dl_i18n.localizer_base import Localizer

from dl_connector_promql.api.connection_info import PromQLConnectionInfoProvider
from dl_connector_promql.api.i18n.localizer import Translatable
from dl_connector_promql.core.constants import PromQLAuthType


@unique
class PromQLFormFieldName(FormFieldName):
    auth_type = "auth_type"
    auth_header = "auth_header"


@attr.s
class PromQLRowConstructor(RowConstructor):
    _localizer: Localizer = attr.ib()

    def _auth_type_options(self) -> list[C.SelectableOption]:
        return [
            C.SelectableOption(
                text=self._localizer.translate(Translatable("value_auth-type-password")),
                value=PromQLAuthType.password.value,
            ),
            C.SelectableOption(
                text=self._localizer.translate(Translatable("value_auth-type-header")),
                value=PromQLAuthType.header.value,
            ),
        ]

    def auth_type_row(
        self,
        default_value: str = PromQLAuthType.password.value,
        display_conditions: TDisplayConditions | None = None,
    ) -> C.CustomizableRow:
        return C.CustomizableRow(
            items=[
                C.LabelRowItem(
                    text=self._localizer.translate(Translatable("field_auth-type")),
                    display_conditions=display_conditions,
                ),
                C.RadioButtonRowItem(
                    name=PromQLFormFieldName.auth_type,
                    options=self._auth_type_options(),
                    default_value=default_value,
                    display_conditions=display_conditions,
                ),
            ]
        )

    def auth_header_row(
        self,
        mode: ConnectionFormMode,
        display_conditions: TDisplayConditions | None = None,
    ) -> C.CustomizableRow:
        return C.CustomizableRow(
            items=[
                C.LabelRowItem(
                    text=self._localizer.translate(Translatable("field_auth-header")),
                    display_conditions=display_conditions,
                    # TODO: add with docs
                    # help_text=self._localizer.translate(Translatable("help_text_field-auth-header")),
                ),
                C.InputRowItem(
                    name=PromQLFormFieldName.auth_header,
                    width="l",
                    default_value="" if mode == ConnectionFormMode.create else None,
                    fake_value="******" if mode == ConnectionFormMode.edit else None,
                    control_props=C.InputRowItem.Props(type="password"),
                    display_conditions=display_conditions,
                ),
            ]
        )


class PromQLConnectionFormFactory(ConnectionFormFactory):
    def _get_common_schema_items(self) -> list[FormFieldApiSchema]:
        return [
            FormFieldApiSchema(name=PromQLFormFieldName.auth_type, required=True),
            FormFieldApiSchema(name=CommonFieldName.host, required=True),
            FormFieldApiSchema(name=CommonFieldName.port, required=True),
            FormFieldApiSchema(name=CommonFieldName.path),
            FormFieldApiSchema(
                name=CommonFieldName.username,
                default_action=FormFieldApiAction.skip,
            ),
            FormFieldApiSchema(
                name=CommonFieldName.password,
                default_action=FormFieldApiAction.skip,
            ),
            FormFieldApiSchema(
                name=PromQLFormFieldName.auth_header,
                required=self.mode == ConnectionFormMode.create,
                default_action=FormFieldApiAction.skip,
            ),
        ]

    def _get_schema_conditions(self) -> list[FormFieldApiActionCondition]:
        return [
            FormFieldApiActionCondition(
                when=FormFieldSelector(name=PromQLFormFieldName.auth_type),
                equals=PromQLAuthType.password.value,
                then=[
                    FormFieldConditionalApiAction(
                        selector=FormFieldSelector(name=CommonFieldName.username),
                        action=FormFieldApiAction.include,
                    ),
                    FormFieldConditionalApiAction(
                        selector=FormFieldSelector(name=CommonFieldName.password),
                        action=FormFieldApiAction.include,
                    ),
                ],
            ),
            FormFieldApiActionCondition(
                when=FormFieldSelector(name=PromQLFormFieldName.auth_type),
                equals=PromQLAuthType.header.value,
                then=[
                    FormFieldConditionalApiAction(
                        selector=FormFieldSelector(name=PromQLFormFieldName.auth_header),
                        action=FormFieldApiAction.include,
                    ),
                ],
            ),
        ]

    def _get_edit_api_schema(
        self,
        connector_settings: ConnectorSettings | None,
    ) -> FormActionApiSchema:
        return FormActionApiSchema(
            items=[
                *self._get_common_schema_items(),
                FormFieldApiSchema(name=CommonFieldName.cache_ttl_sec, nullable=True),
                FormFieldApiSchema(name=CommonFieldName.secure, type="boolean"),
                FormFieldApiSchema(name=CommonFieldName.data_export_forbidden),
            ],
            conditions=self._get_schema_conditions(),
        )

    def _get_create_api_schema(
        self,
        connector_settings: ConnectorSettings | None,
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
        connector_settings: ConnectorSettings | None,
    ) -> FormActionApiSchema:
        return FormActionApiSchema(
            items=[
                *self._get_common_schema_items(),
                FormFieldApiSchema(name=CommonFieldName.secure, type="boolean"),
                *self._get_top_level_check_api_schema_items(),
            ],
            conditions=self._get_schema_conditions(),
        )

    def get_form_config(
        self,
        connector_settings: Optional[ConnectorSettings],
        tenant: Optional[TenantDef],
    ) -> ConnectionForm:
        rc = PromQLRowConstructor(localizer=self._localizer)

        edit_api_schema = self._get_edit_api_schema(connector_settings)
        create_api_schema = self._get_create_api_schema(connector_settings, edit_api_schema)
        check_api_schema = self._get_check_api_schema(connector_settings)

        form_params = self._get_form_params()

        return ConnectionForm(
            title=PromQLConnectionInfoProvider.get_title(self._localizer),
            rows=self._filter_nulls(
                [
                    rc.auth_type_row(),
                    rc.host_row(),
                    rc.port_row(),
                    rc.path_row(),
                    rc.username_row(display_conditions={PromQLFormFieldName.auth_type: PromQLAuthType.password.value}),
                    rc.password_row(
                        self.mode, display_conditions={PromQLFormFieldName.auth_type: PromQLAuthType.password.value}
                    ),
                    rc.auth_header_row(
                        self.mode, display_conditions={PromQLFormFieldName.auth_type: PromQLAuthType.header.value}
                    ),
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
            ),
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
