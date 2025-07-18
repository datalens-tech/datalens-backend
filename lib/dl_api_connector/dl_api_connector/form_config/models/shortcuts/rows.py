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
from dl_constants.enums import (
    RawSQLLevel,
    sort_raw_sql_levels,
)
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
        label_text: BaseTranslatable | None = None,  # noqa: B008
        default_value: Optional[str] = None,
        display_conditions: Optional[TDisplayConditions] = None,
        disabled: Optional[bool] = None,
    ) -> C.CustomizableRow:
        if label_text is None:
            label_text = Translatable("field_port")
        text = self._localizer.translate(label_text)
        return C.CustomizableRow(
            items=[
                C.LabelRowItem(
                    text=text,
                    display_conditions=display_conditions,
                ),
                C.InputRowItem(
                    name=CommonFieldName.port,
                    width="s",
                    control_props=C.InputRowItem.Props(type="number", disabled=disabled),
                    default_value=default_value,
                    display_conditions=display_conditions,
                ),
            ]
        )

    def path_row(
        self,
        default_value: Optional[str] = None,
        display_conditions: Optional[TDisplayConditions] = None,
        label_help_text: Optional[str] = None,
    ) -> C.CustomizableRow:
        text = self._localizer.translate(Translatable("field_path"))
        return C.CustomizableRow(
            items=[
                C.LabelRowItem(text=text, display_conditions=display_conditions, help_text=label_help_text),
                C.InputRowItem(
                    name=CommonFieldName.path,
                    width="l",
                    default_value=default_value,
                    display_conditions=display_conditions,
                ),
            ]
        )

    def username_row(
        self,
        label_text: BaseTranslatable = Translatable("field_username"),  # noqa: B008
        default_value: Optional[str] = None,
        display_conditions: Optional[TDisplayConditions] = None,
        control_props: Optional[C.InputRowItem.Props] = None,
        inner: Optional[bool] = None,
    ) -> C.CustomizableRow:
        return C.CustomizableRow(
            items=[
                C.LabelRowItem(text=self._localizer.translate(label_text), display_conditions=display_conditions),
                C.InputRowItem(
                    name=CommonFieldName.username,
                    default_value=default_value,
                    display_conditions=display_conditions,
                    control_props=control_props,
                    inner=inner,
                    width="m",
                ),
            ]
        )

    def password_row(
        self,
        mode: ConnectionFormMode,
        display_conditions: Optional[TDisplayConditions] = None,
    ) -> C.CustomizableRow:
        return C.CustomizableRow(
            items=[
                C.LabelRowItem(
                    text=self._localizer.translate(Translatable("field_password")),
                    display_conditions=display_conditions,
                ),
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

    def db_name_row(
        self,
        label_text: BaseTranslatable = Translatable("field_db-name"),  # noqa: B008
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

    def _get_raw_sql_level_radio_group_option_text(
        self,
        levels: list[RawSQLLevel],
    ) -> str:
        if len(levels) == 1:
            template = self._localizer.translate(Translatable("value_raw-sql-level-template-1"))
        elif len(levels) == 2:
            template = self._localizer.translate(Translatable("value_raw-sql-level-template-2"))
        elif len(levels) == 3:
            template = self._localizer.translate(Translatable("value_raw-sql-level-template-3"))
        else:
            raise ValueError("Invalid number of levels")

        values = []
        for level in levels:
            if level == RawSQLLevel.subselect:
                level_text = self._localizer.translate(Translatable("value_raw-sql-level-value-subselect"))
            elif level == RawSQLLevel.template:
                level_text = self._localizer.translate(Translatable("value_raw-sql-level-value-template"))
            elif level == RawSQLLevel.dashsql:
                level_text = self._localizer.translate(Translatable("value_raw-sql-level-value-dashsql"))
            else:
                raise ValueError("Invalid level")

            values.append(level_text)

        return template.format(*values)

    def _get_raw_sql_level_radio_group_option_hint_text(
        self,
        levels: list[RawSQLLevel],
    ) -> str:
        if len(levels) == 1:
            template = self._localizer.translate(Translatable("value_raw-sql-level-hint-template-1"))
        elif len(levels) == 2:
            template = self._localizer.translate(Translatable("value_raw-sql-level-hint-template-2"))
        elif len(levels) == 3:
            template = self._localizer.translate(Translatable("value_raw-sql-level-hint-template-3"))
        else:
            raise ValueError("Invalid number of levels")

        values = []
        for level in levels:
            if level == RawSQLLevel.subselect:
                level_text = self._localizer.translate(Translatable("value_raw-sql-level-value-hint-subselect"))
            elif level == RawSQLLevel.template:
                level_text = self._localizer.translate(Translatable("value_raw-sql-level-value-hint-template"))
            elif level == RawSQLLevel.dashsql:
                level_text = self._localizer.translate(Translatable("value_raw-sql-level-value-hint-dashsql"))
            else:
                raise ValueError("Invalid level")

            values.append(level_text)

        return template.format(*values)

    def _get_raw_sql_level_radio_group_option_text_end_icon(
        self,
        raw_sql_level: RawSQLLevel,
    ) -> Optional[C.RadioGroupRowItemOption.ValueContent.TextEndIcon]:
        if raw_sql_level == RawSQLLevel.dashsql:
            return C.RadioGroupRowItemOption.ValueContent.TextEndIcon(
                name=C.RadioGroupRowItemOption.ValueContent.TextEndIcon.Name.circle_exclamation,
                view=C.RadioGroupRowItemOption.ValueContent.TextEndIcon.View.error,
            )

        return None

    def _raw_sql_level_to_radio_group_option(
        self,
        raw_sql_level: RawSQLLevel,
        all_raw_sql_levels: list[RawSQLLevel],
        with_text_end_icon: bool = True,
    ) -> C.RadioGroupRowItemOption:
        if raw_sql_level == RawSQLLevel.off:
            return C.RadioGroupRowItemOption(
                value=raw_sql_level.value,
                content=C.RadioGroupRowItemOption.ValueContent(
                    text=self._localizer.translate(Translatable("value_raw-sql-level-off")),
                ),
            )
        all_raw_sql_levels = sort_raw_sql_levels(all_raw_sql_levels)

        for i, level in enumerate(all_raw_sql_levels):
            if level == raw_sql_level:
                included_levels = all_raw_sql_levels[: i + 1]
                break
        else:
            raise ValueError(f"RawSQLLevel {raw_sql_level} not found in all_raw_sql_levels {all_raw_sql_levels}")

        content_text = self._get_raw_sql_level_radio_group_option_text(included_levels)
        content_hint_text = self._get_raw_sql_level_radio_group_option_hint_text(included_levels)
        if with_text_end_icon:
            text_end_icon = self._get_raw_sql_level_radio_group_option_text_end_icon(raw_sql_level)
        else:
            text_end_icon = None

        return C.RadioGroupRowItemOption(
            value=raw_sql_level.value,
            content=C.RadioGroupRowItemOption.ValueContent(
                text=content_text,
                hint_text=content_hint_text,
                text_end_icon=text_end_icon,
            ),
        )

    def _get_raw_sql_level_radio_group_options(
        self,
        raw_sql_levels: list[RawSQLLevel],
        with_text_end_icon: bool = True,
    ) -> list[C.RadioGroupRowItemOption]:
        return [
            self._raw_sql_level_to_radio_group_option(
                raw_sql_level=raw_sql_level,
                all_raw_sql_levels=raw_sql_levels,
                with_text_end_icon=with_text_end_icon,
            )
            for raw_sql_level in raw_sql_levels
        ]

    def raw_sql_level_row_v2(
        self,
        default_value: RawSQLLevel = RawSQLLevel.off,
        switch_off_value: RawSQLLevel = RawSQLLevel.off,
        raw_sql_levels: Optional[list[RawSQLLevel]] = None,
    ) -> C.RawSqlLevelRow:
        if raw_sql_levels is None:
            raw_sql_levels = [RawSQLLevel.subselect, RawSQLLevel.template, RawSQLLevel.dashsql]

        raw_sql_levels = sort_raw_sql_levels(raw_sql_levels)

        if switch_off_value in raw_sql_levels:
            raise ValueError("switch_off_value must not be in raw_sql_levels")

        if default_value not in raw_sql_levels and switch_off_value != default_value:
            raise ValueError("default_value must be in raw_sql_levels or equal switch_off_value")

        label_allow_subselect_hint_2 = self._localizer.translate(Translatable("label_allow-subselect-hint-2"))
        label_allow_subselect_hint_3 = self._localizer.translate(Translatable("label_allow-subselect-hint-3"))

        return C.RawSqlLevelRow(
            name=CommonFieldName.raw_sql_level,
            default_value=default_value.value,
            switch_off_value=switch_off_value.value,
            label=C.LabelRowItem(
                align="start",
                text=self._localizer.translate(Translatable("field_raw-sql-level")),
                help_text=f"- {label_allow_subselect_hint_2}\n- {label_allow_subselect_hint_3}",
            ),
            radio_group=C.RadioGroupRowItem(
                name=CommonFieldName.raw_sql_level,
                options=self._get_raw_sql_level_radio_group_options(raw_sql_levels),
            ),
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
        display_conditions: Optional[TDisplayConditions] = None,
    ) -> typing.Sequence[C.CustomizableRow]:
        if display_conditions is None:
            display_conditions = {}
        return [
            C.CustomizableRow(
                items=[
                    C.LabelRowItem(
                        text=self._localizer.translate(Translatable("label_ssl-enabled")),
                        display_conditions={CommonFieldName.advanced_settings: "opened"} | display_conditions,
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
                        display_conditions={CommonFieldName.advanced_settings: "opened"} | display_conditions,
                    ),
                ]
            ),
            C.CustomizableRow(
                items=[
                    C.LabelRowItem(
                        text=self._localizer.translate(Translatable("label_ssl-ca")),
                        display_conditions={CommonFieldName.advanced_settings: "opened"} | display_conditions,
                    ),
                    C.FileInputRowItem(
                        name=CommonFieldName.ssl_ca,
                        display_conditions={CommonFieldName.advanced_settings: "opened"} | display_conditions,
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
