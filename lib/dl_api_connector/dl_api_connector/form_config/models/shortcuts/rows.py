import typing
from typing import Optional

import attr

from dl_api_connector.form_config.models import rows as C
from dl_api_connector.form_config.models.base import ConnectionFormMode
from dl_api_connector.form_config.models.common import (
    BooleanField,
    CommonFieldName,
    MarkdownStr,
)
from dl_api_connector.form_config.models.rows.base import TDisplayConditions
from dl_api_connector.i18n.localizer import Translatable
from dl_constants.enums import RawSQLLevel
from dl_i18n.localizer_base import Localizer
from dl_i18n.localizer_base import Translatable as BaseTranslatable


@attr.s
class RowConstructor:
    _localizer: Localizer = attr.ib()

    def host_row(
        self,
        default_value: Optional[str] = None,
        display_conditions: Optional[TDisplayConditions] = None,
        label_help_text: Optional[str] = None,
    ) -> C.CustomizableRow:
        text = self._localizer.translate(Translatable("field_host"))
        return C.CustomizableRow(
            items=[
                C.LabelRowItem(text=text, display_conditions=display_conditions, help_text=label_help_text),
                C.InputRowItem(
                    name=CommonFieldName.host,
                    width="l",
                    default_value=default_value,
                    display_conditions=display_conditions,
                ),
            ]
        )

    def port_row(
        self,
        label_text: BaseTranslatable = Translatable("field_port"),
        default_value: Optional[str] = None,
    ) -> C.CustomizableRow:
        text = self._localizer.translate(label_text)
        return C.CustomizableRow(
            items=[
                C.LabelRowItem(text=text),
                C.InputRowItem(
                    name=CommonFieldName.port,
                    width="s",
                    control_props=C.InputRowItem.Props(type="number"),
                    default_value=default_value,
                ),
            ]
        )

    def username_row(
        self,
        label_text: BaseTranslatable = Translatable("field_username"),
        default_value: Optional[str] = None,
        display_conditions: Optional[TDisplayConditions] = None,
    ) -> C.CustomizableRow:
        return C.CustomizableRow(
            items=[
                C.LabelRowItem(text=self._localizer.translate(label_text), display_conditions=display_conditions),
                C.InputRowItem(
                    name=CommonFieldName.username,
                    width="m",
                    default_value=default_value,
                    display_conditions=display_conditions,
                ),
            ]
        )

    def password_row(self, mode: ConnectionFormMode) -> C.CustomizableRow:
        label_text = self._localizer.translate(Translatable("field_password"))
        return C.CustomizableRow(
            items=[
                C.LabelRowItem(text=label_text),
                C.InputRowItem(
                    name=CommonFieldName.password,
                    width="m",
                    default_value="" if mode == ConnectionFormMode.create else None,
                    fake_value="******" if mode == ConnectionFormMode.edit else None,
                    control_props=C.InputRowItem.Props(type="password"),
                ),
            ]
        )

    def db_name_row(
        self,
        label_text: BaseTranslatable = Translatable("field_db-name"),
        default_value: Optional[str] = None,
        display_conditions: Optional[TDisplayConditions] = None,
    ) -> C.CustomizableRow:
        return C.CustomizableRow(
            items=[
                C.LabelRowItem(text=self._localizer.translate(label_text), display_conditions=display_conditions),
                C.InputRowItem(
                    name=CommonFieldName.db_name,
                    width="l",
                    default_value=default_value,
                    display_conditions=display_conditions,
                ),
            ]
        )

    def raw_sql_level_to_radio_group_option(
        self,
        raw_sql_level: RawSQLLevel,
    ) -> C.RadioGroupRowItemOption:
        return {
            RawSQLLevel.off: C.RadioGroupRowItemOption(
                value=raw_sql_level.value,
                content=C.RadioGroupRowItemOption.ValueContent(
                    text=self._localizer.translate(Translatable("value_raw-sql-level-off")),
                ),
            ),
            RawSQLLevel.subselect: C.RadioGroupRowItemOption(
                value=raw_sql_level.value,
                content=C.RadioGroupRowItemOption.ValueContent(
                    text=self._localizer.translate(Translatable("value_raw-sql-level-subselect")),
                    hint_text=self._localizer.translate(Translatable("value_raw-sql-level-subselect-note")),
                ),
            ),
            RawSQLLevel.dashsql: C.RadioGroupRowItemOption(
                value=raw_sql_level.value,
                content=C.RadioGroupRowItemOption.ValueContent(
                    text=self._localizer.translate(Translatable("value_raw-sql-level-dashsql")),
                    hint_text=self._localizer.translate(Translatable("value_raw-sql-level-dashsql-note")),
                ),
            ),
        }[raw_sql_level]

    def _raw_sql_level_default_options(self) -> list[C.RadioGroupRowItemOption]:
        return [
            self.raw_sql_level_to_radio_group_option(raw_sql_level)
            for raw_sql_level in (RawSQLLevel.off, RawSQLLevel.subselect, RawSQLLevel.dashsql)
        ]

    def raw_sql_level_row(
        self,
        default_value: RawSQLLevel = RawSQLLevel.off,
        options: Optional[list[C.RadioGroupRowItemOption]] = None,
        disabled: Optional[bool] = None,
    ) -> C.CustomizableRow:
        label_allow_subselect_hint_2 = self._localizer.translate(Translatable("label_allow-subselect-hint-2"))
        label_allow_subselect_hint_3 = self._localizer.translate(Translatable("label_allow-subselect-hint-3"))
        return C.CustomizableRow(
            items=[
                C.LabelRowItem(
                    align="start",
                    text=self._localizer.translate(Translatable("field_raw-sql-level")),
                    help_text=f"- {label_allow_subselect_hint_2}\n- {label_allow_subselect_hint_3}",
                ),
                C.RadioGroupRowItem(
                    name=CommonFieldName.raw_sql_level,
                    options=options if options is not None else self._raw_sql_level_default_options(),
                    default_value=self.raw_sql_level_to_radio_group_option(default_value).value,
                    control_props=C.RadioGroupRowItem.Props(disabled=disabled),
                ),
            ]
        )

    def access_token_input_row(
        self,
        mode: ConnectionFormMode,
        label_help_text: Optional[MarkdownStr] = None,
    ) -> C.CustomizableRow:
        return C.CustomizableRow(
            items=[
                C.LabelRowItem(
                    text=self._localizer.translate(Translatable("field_access-token")),
                    help_text=label_help_text,
                ),
                C.InputRowItem(
                    name=CommonFieldName.access_token,
                    width="l",
                    control_props=C.InputRowItem.Props(multiline=True),
                    fake_value="******" if mode == ConnectionFormMode.edit else None,
                ),
            ]
        )

    def auto_create_dash_row(self, default_value: Optional[bool] = True) -> C.CustomizableRow:
        return C.CustomizableRow(
            items=[
                C.CheckboxRowItem(
                    name=CommonFieldName.is_auto_create_dashboard,
                    text=self._localizer.translate(Translatable("field_auto-create-dashboard")),
                    inner=True,
                    default_value=default_value,
                    control_props=C.CheckboxRowItem.Props(qa="conn-auto-create-dash-checkbox"),
                ),
            ]
        )

    def collapse_advanced_settings_row(self) -> C.CollapseRow:
        return C.CollapseRow(
            inner=True,
            name=CommonFieldName.advanced_settings,
            text=self._localizer.translate(Translatable("label_advanced-settings")),
        )

    def ssl_rows(
        self,
        enabled_name: CommonFieldName,
        enabled_help_text: str,
        enabled_default_value: bool,
    ) -> typing.Sequence[C.CustomizableRow]:
        return [
            C.CustomizableRow(
                items=[
                    C.LabelRowItem(
                        text=self._localizer.translate(Translatable("label_ssl-enabled")),
                        display_conditions={CommonFieldName.advanced_settings: "opened"},
                        help_text=enabled_help_text,
                    ),
                    C.RadioButtonRowItem(
                        name=enabled_name,
                        options=[
                            C.SelectableOption(
                                text=self._localizer.translate(Translatable("value_ssl-enabled-off")),
                                value=BooleanField.off.value,
                            ),
                            C.SelectableOption(
                                text=self._localizer.translate(Translatable("value_ssl-enabled-on")),
                                value=BooleanField.on.value,
                            ),
                        ],
                        default_value=BooleanField.on.value if enabled_default_value else BooleanField.off.value,
                        display_conditions={CommonFieldName.advanced_settings: "opened"},
                    ),
                ]
            ),
            C.CustomizableRow(
                items=[
                    C.LabelRowItem(
                        text=self._localizer.translate(Translatable("label_ssl-ca")),
                        display_conditions={CommonFieldName.advanced_settings: "opened"},
                    ),
                    C.FileInputRowItem(
                        name=CommonFieldName.ssl_ca,
                        display_conditions={CommonFieldName.advanced_settings: "opened"},
                    ),
                ]
            ),
        ]

    def data_export_forbidden_row(self) -> C.CustomizableRow:
        return C.CustomizableRow(
            items=[
                C.LabelRowItem(
                    text=self._localizer.translate(Translatable("label_data-export-forbidden")),
                    display_conditions={CommonFieldName.advanced_settings: "opened"},
                    help_text=self._localizer.translate(Translatable("help_text_data-export-forbidden")),
                ),
                C.RadioButtonRowItem(
                    name=CommonFieldName.data_export_forbidden,
                    options=[
                        C.SelectableOption(
                            text=self._localizer.translate(Translatable("value_data-export-forbidden-off")),
                            value=BooleanField.off.value,
                        ),
                        C.SelectableOption(
                            text=self._localizer.translate(Translatable("value_data-export-forbidden-on")),
                            value=BooleanField.on.value,
                        ),
                    ],
                    default_value=BooleanField.off.value,
                    display_conditions={CommonFieldName.advanced_settings: "opened"},
                ),
            ]
        )
