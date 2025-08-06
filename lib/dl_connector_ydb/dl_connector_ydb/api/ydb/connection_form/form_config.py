from __future__ import annotations

from enum import (
    Enum,
    unique,
)
from typing import (
    Optional,
    Sequence,
)

import attr

from dl_api_commons.base_models import (
    FormConfigParams,
    TenantDef,
)
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
)
from dl_api_connector.form_config.models.common import (
    CommonFieldName,
    FormFieldName,
    OAuthApplication,
)
import dl_api_connector.form_config.models.rows as C
from dl_api_connector.form_config.models.rows.base import (
    FormRow,
    TDisplayConditions,
)
from dl_api_connector.form_config.models.shortcuts.rows import RowConstructor
from dl_configs.connectors_settings import ConnectorSettingsBase
from dl_constants.enums import RawSQLLevel
from dl_i18n.localizer_base import Localizer

from dl_connector_ydb.api.ydb.connection_info import YDBConnectionInfoProvider
from dl_connector_ydb.api.ydb.i18n.localizer import Translatable
from dl_connector_ydb.core.ydb.constants import YDBAuthTypeMode
from dl_connector_ydb.core.ydb.settings import YDBConnectorSettings


class YDBOAuthApplication(OAuthApplication):
    ydb = "ydb"


@unique
class YDBFieldName(FormFieldName):
    auth_type = "auth_type"


@attr.s
class YDBRowConstructor(RowConstructor):
    _localizer: Localizer = attr.ib()

    def auth_type_row(self, mode: ConnectionFormMode) -> C.CustomizableRow:
        return C.CustomizableRow(
            items=[
                C.LabelRowItem(
                    text=self._localizer.translate(Translatable("field_auth-type")),
                ),
                C.RadioButtonRowItem(
                    name=YDBFieldName.auth_type,
                    options=[
                        C.SelectableOption(
                            text=self._localizer.translate(Translatable("value_auth-type-anonymous")),
                            value=YDBAuthTypeMode.anonymous.value,
                        ),
                        C.SelectableOption(
                            text=self._localizer.translate(Translatable("value_auth-type-password")),
                            value=YDBAuthTypeMode.password.value,
                        ),
                        C.SelectableOption(
                            text=self._localizer.translate(Translatable("value_auth-type-oauth")),
                            value=YDBAuthTypeMode.oauth.value,
                        ),
                    ],
                    default_value=YDBAuthTypeMode.oauth.value,
                ),
            ]
        )

    def password_row(
        self, mode: ConnectionFormMode, display_conditions: Optional[TDisplayConditions] = None
    ) -> C.CustomizableRow:
        label_text = self._localizer.translate(Translatable("field_password"))
        return C.CustomizableRow(
            items=[
                C.LabelRowItem(text=label_text, display_conditions=display_conditions),
                C.InputRowItem(
                    name=CommonFieldName.token,
                    width="m",
                    default_value="" if mode == ConnectionFormMode.create else None,
                    fake_value="******" if mode == ConnectionFormMode.edit else None,
                    control_props=C.InputRowItem.Props(type="password"),
                    display_conditions=display_conditions,
                ),
            ]
        )


class YDBConnectionFormFactory(ConnectionFormFactory):
    def _get_base_common_api_schema_items(self, names_source: type[Enum]) -> list[FormFieldApiSchema]:
        return [
            FormFieldApiSchema(name=names_source.host, required=True),  # type: ignore  # 2024-01-24 # TODO: "type[Enum]" has no attribute "host"  [attr-defined]
            FormFieldApiSchema(name=names_source.port, required=True),  # type: ignore  # 2024-01-24 # TODO: "type[Enum]" has no attribute "port"  [attr-defined]
            FormFieldApiSchema(name=names_source.db_name, required=True),  # type: ignore  # 2024-01-24 # TODO: "type[Enum]" has no attribute "db_name"  [attr-defined]
            FormFieldApiSchema(name=CommonFieldName.ssl_enable),
            FormFieldApiSchema(name=CommonFieldName.ssl_ca),
            FormFieldApiSchema(name=CommonFieldName.data_export_forbidden),
        ]

    def _get_default_db_section(self, rc: RowConstructor, connector_settings: YDBConnectorSettings) -> list[FormRow]:
        oauth_row = (
            C.OAuthTokenRow(
                name=CommonFieldName.token,
                fake_value="******" if self.mode == ConnectionFormMode.edit else None,
                application=YDBOAuthApplication.ydb,
            )
            if not connector_settings.ENABLE_AUTH_TYPE_PICKER
            else C.CustomizableRow(
                items=[
                    C.LabelRowItem(
                        text=self._localizer.translate(
                            Translatable("field_oauth_row"),
                        ),
                        display_conditions={YDBFieldName.auth_type: YDBAuthTypeMode.oauth.value},
                    ),
                    C.InputRowItem(
                        name=CommonFieldName.token,
                        width="l",
                        default_value=None,
                        fake_value="******" if self.mode == ConnectionFormMode.edit else None,
                        control_props=C.InputRowItem.Props(type="password"),
                        display_conditions={YDBFieldName.auth_type: YDBAuthTypeMode.oauth.value},
                    ),
                ]
            )
        )
        return [
            oauth_row,
            rc.host_row(default_value=connector_settings.DEFAULT_HOST_VALUE),
            rc.port_row(default_value="2135"),
            rc.db_name_row(),
        ]

    def _get_base_edit_api_schema(self) -> FormActionApiSchema:
        return FormActionApiSchema(
            items=[
                FormFieldApiSchema(name=CommonFieldName.cache_ttl_sec, nullable=True),
                FormFieldApiSchema(name=CommonFieldName.raw_sql_level),
            ]
        )

    def _get_base_create_api_schema(self, edit_api_schema: FormActionApiSchema) -> FormActionApiSchema:
        return FormActionApiSchema(
            items=[
                *edit_api_schema.items,
                *self._get_top_level_create_api_schema_items(),
            ]
        )

    def _get_base_check_api_schema(self, common_api_schema_items: list[FormFieldApiSchema]) -> FormActionApiSchema:
        return FormActionApiSchema(
            items=[
                *common_api_schema_items,
                *self._get_top_level_check_api_schema_items(),
            ]
        )

    def _get_base_form_config(
        self,
        db_section_rows: Sequence[FormRow],
        create_api_schema: FormActionApiSchema,
        edit_api_schema: FormActionApiSchema,
        check_api_schema: FormActionApiSchema,
        rc: RowConstructor,
        ydb_rc: YDBRowConstructor,
        connector_settings: Optional[ConnectorSettingsBase],
    ) -> ConnectionForm:
        assert connector_settings is not None and isinstance(connector_settings, YDBConnectorSettings)

        raw_sql_levels = [RawSQLLevel.subselect, RawSQLLevel.dashsql]
        if connector_settings.ENABLE_DATASOURCE_TEMPLATE:
            raw_sql_levels.append(RawSQLLevel.template)

        if connector_settings.ENABLE_AUTH_TYPE_PICKER:
            rows = [
                ydb_rc.auth_type_row(mode=self.mode),
                *db_section_rows,
                rc.username_row(
                    display_conditions={YDBFieldName.auth_type: YDBAuthTypeMode.password.value},
                ),
                ydb_rc.password_row(
                    display_conditions={YDBFieldName.auth_type: YDBAuthTypeMode.password.value}, mode=self.mode
                ),
                C.CacheTTLRow(name=CommonFieldName.cache_ttl_sec),
                rc.raw_sql_level_row_v2(raw_sql_levels=raw_sql_levels),
                rc.collapse_advanced_settings_row(),
                *rc.ssl_rows(
                    enabled_name=CommonFieldName.ssl_enable,
                    enabled_help_text=self._localizer.translate(Translatable("label_ydb-ssl-enabled-tooltip")),
                    enabled_default_value=connector_settings.DEFAULT_SSL_ENABLE_VALUE,
                ),
                rc.data_export_forbidden_row(),
            ]
        else:
            rows = [
                *db_section_rows,
                C.CacheTTLRow(name=CommonFieldName.cache_ttl_sec),
                rc.raw_sql_level_row_v2(raw_sql_levels=raw_sql_levels),
                rc.collapse_advanced_settings_row(),
                *rc.ssl_rows(
                    enabled_name=CommonFieldName.ssl_enable,
                    enabled_help_text=self._localizer.translate(Translatable("label_ydb-ssl-enabled-tooltip")),
                    enabled_default_value=connector_settings.DEFAULT_SSL_ENABLE_VALUE,
                ),
                rc.data_export_forbidden_row(),
            ]
        return ConnectionForm(
            title=YDBConnectionInfoProvider.get_title(self._localizer),
            rows=rows,
            api_schema=FormApiSchema(
                create=create_api_schema if self.mode == ConnectionFormMode.create else None,
                edit=edit_api_schema if self.mode == ConnectionFormMode.edit else None,
                check=check_api_schema,
            ),
        )

    def _get_extra_items(self) -> list[FormFieldApiSchema]:
        return [
            FormFieldApiSchema(
                name=CommonFieldName.token,
                required=self.mode == ConnectionFormMode.create,
                default_action=FormFieldApiAction.include,
            ),
            FormFieldApiSchema(name=CommonFieldName.username, required=True, default_action=FormFieldApiAction.skip),
            FormFieldApiSchema(name=YDBFieldName.auth_type, required=True),
        ]

    def _get_extra_conditions(self) -> list[FormFieldApiActionCondition]:
        return [
            FormFieldApiActionCondition(
                when=FormFieldSelector(name=YDBFieldName.auth_type),
                equals=YDBAuthTypeMode.password.value,
                then=[
                    FormFieldConditionalApiAction(
                        selector=FormFieldSelector(name=CommonFieldName.username),
                        action=FormFieldApiAction.include,
                    ),
                ],
            ),
            FormFieldApiActionCondition(
                when=FormFieldSelector(name=YDBFieldName.auth_type),
                equals=YDBAuthTypeMode.anonymous.value,
                then=[
                    FormFieldConditionalApiAction(
                        selector=FormFieldSelector(name=CommonFieldName.token),
                        action=FormFieldApiAction.skip,
                    ),
                ],
            ),
        ]

    def get_form_config(
        self,
        connector_settings: Optional[ConnectorSettingsBase],
        tenant: Optional[TenantDef],
        params: Optional[FormConfigParams] = None,
    ) -> ConnectionForm:
        assert connector_settings is not None and isinstance(connector_settings, YDBConnectorSettings)
        rc = RowConstructor(localizer=self._localizer)
        ydb_rc = YDBRowConstructor(localizer=self._localizer)

        edit_api_schema = self._get_base_edit_api_schema()
        common_api_schema_items = self._get_base_common_api_schema_items(names_source=CommonFieldName)
        db_section_rows = self._get_default_db_section(rc=rc, connector_settings=connector_settings)
        if not connector_settings.ENABLE_AUTH_TYPE_PICKER:
            common_api_schema_items.append(
                FormFieldApiSchema(
                    name=CommonFieldName.token,
                    required=self.mode == ConnectionFormMode.create,
                )
            )
        edit_api_schema.items.extend(common_api_schema_items)

        create_api_schema = self._get_base_create_api_schema(edit_api_schema=edit_api_schema)
        check_api_schema = self._get_base_check_api_schema(common_api_schema_items=common_api_schema_items)

        if connector_settings.ENABLE_AUTH_TYPE_PICKER:
            extra_api_items = self._get_extra_items()
            extra_api_conditions = self._get_extra_conditions()

            for schema in [edit_api_schema, check_api_schema, create_api_schema]:
                schema.items.extend(extra_api_items)
                schema.conditions.extend(extra_api_conditions)

        return self._get_base_form_config(
            db_section_rows=db_section_rows,
            create_api_schema=create_api_schema,
            edit_api_schema=edit_api_schema,
            check_api_schema=check_api_schema,
            rc=rc,
            ydb_rc=ydb_rc,
            connector_settings=connector_settings,
        )
