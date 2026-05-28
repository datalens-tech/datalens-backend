import enum
from typing import (
    Literal,
)

import attr

from dl_api_connector.form_config.models.common import (
    Align,
    MarkdownStr,
    SerializableConfig,
    Width,
    inner,
    remap_skip_if_null,
    skip_if_null,
)
from dl_api_connector.form_config.models.rows.base import (
    DisplayConditionsMixin,
    FormFieldMixin,
    InnerFieldMixin,
)
from dl_api_connector.form_config.models.rows.customizable.base import (
    ControlRowItem,
    DefaultValueMixin,
    PlaceholderMixin,
    RowItem,
)


@attr.s(kw_only=True, frozen=True)
class HiddenRowItem(RowItem, FormFieldMixin, InnerFieldMixin, DisplayConditionsMixin, DefaultValueMixin):
    """
    Can be used to send values that do not have a corresponding control item
    (e.g. calculated token expiration time, that is received from an external API)
    """

    component_id = "hidden"


@attr.s(kw_only=True, frozen=True)
class LabelRowItem(RowItem, DisplayConditionsMixin):
    component_id = "label"
    text: str = attr.ib()
    align: Align | None = attr.ib(default=None, metadata=skip_if_null())
    help_text: MarkdownStr | None = attr.ib(default=None, metadata=remap_skip_if_null("helpText"))


@attr.s(kw_only=True, frozen=True)
class SelectableOption:  # TODO rename to RadioButtonOption
    text: str = attr.ib()
    value: str = attr.ib()


@attr.s(kw_only=True, frozen=True)
class InputRowItem(ControlRowItem, PlaceholderMixin):
    @attr.s(kw_only=True, frozen=True)
    class Props(SerializableConfig):
        multiline: bool | None = attr.ib(default=None, metadata=skip_if_null())  # false if undefined
        type: Literal["text", "password", "number"] | None = attr.ib(default="text", metadata=skip_if_null())
        disabled: bool | None = attr.ib(default=None, metadata=skip_if_null())

    component_id = "input"

    control_props: Props | None = attr.ib(default=None, metadata=remap_skip_if_null("controlProps"))


@attr.s(kw_only=True, frozen=True)
class SelectOption(SerializableConfig):
    class Data(SerializableConfig):
        description: str | None = attr.ib(default=None, metadata=skip_if_null())

    content: str = attr.ib()
    value: str = attr.ib()
    data: Data | None = attr.ib(default=None, metadata=skip_if_null())


@attr.s(kw_only=True, frozen=True)
class SelectRowItem(ControlRowItem, PlaceholderMixin):
    @attr.s(kw_only=True, frozen=True)
    class Props(SerializableConfig):
        show_search: bool | None = attr.ib(default=None, metadata=remap_skip_if_null("showSearch"))
        has_clear: bool | None = attr.ib(default=None, metadata=remap_skip_if_null("hasClear"))
        multiple: bool | None = attr.ib(default=None, metadata=skip_if_null())

    component_id = "select"

    available_values: list[SelectOption] | None = attr.ib(default=None, metadata=remap_skip_if_null("availableValues"))
    control_props: Props | None = attr.ib(default=None, metadata=remap_skip_if_null("controlProps"))


@attr.s(kw_only=True, frozen=True)
class RadioButtonRowItem(ControlRowItem):
    component_id = "radio_button"

    options: list[SelectableOption] = attr.ib()


@attr.s(kw_only=True, frozen=True)
class RadioGroupRowItemOption(SerializableConfig):
    @attr.s(kw_only=True, frozen=True)
    class ValueContent(SerializableConfig):
        @attr.s(kw_only=True, frozen=True)
        class TextEndIcon(SerializableConfig):
            class Name(enum.Enum):
                circle_exclamation = "CircleExclamation"

            class View(enum.Enum):
                error = "error"

            name: Name = attr.ib()
            view: View = attr.ib()

        text: str = attr.ib()
        hint_text: MarkdownStr | None = attr.ib(default=None, metadata=remap_skip_if_null("hintText"))
        text_end_icon: TextEndIcon | None = attr.ib(default=None, metadata=remap_skip_if_null("textEndIcon"))

    content: ValueContent = attr.ib()
    value: str = attr.ib()


@attr.s(kw_only=True, frozen=True)
class RadioGroupRowItem(RowItem, DisplayConditionsMixin, FormFieldMixin, InnerFieldMixin):
    @attr.s(kw_only=True, frozen=True)
    class Props(SerializableConfig):
        disabled: bool | None = attr.ib(default=None, metadata=skip_if_null())

    component_id = "radio_group"

    options: list[RadioGroupRowItemOption] = attr.ib()
    default_value: str | None = attr.ib(default=None, metadata=remap_skip_if_null("defaultValue"))
    control_props: Props | None = attr.ib(default=None, metadata=remap_skip_if_null("controlProps"))

    def __attrs_post_init__(self) -> None:
        if self.default_value is not None and self.default_value not in (
            possible_values := [option.value for option in self.options]
        ):
            raise ValueError(f"Invalid default value for radio group: {self.default_value} is not in {possible_values}")


@attr.s(kw_only=True, frozen=True)
class CheckboxRowItem(RowItem, DisplayConditionsMixin, FormFieldMixin, InnerFieldMixin, DefaultValueMixin):
    @attr.s(kw_only=True, frozen=True)
    class Props(SerializableConfig):
        disabled: bool | None = attr.ib(default=None, metadata=skip_if_null())
        size: Literal["m", "l"] | None = attr.ib(default=None, metadata=skip_if_null())
        qa: str | None = attr.ib(
            default=None, metadata=skip_if_null()
        )  # UI-specific testing stuff, should be removed at some point

    component_id = "checkbox"

    text: str = attr.ib()
    control_props: Props | None = attr.ib(default=None, metadata=remap_skip_if_null("controlProps"))


@attr.s(kw_only=True, frozen=True)
class DatepickerRowItem(ControlRowItem):
    @attr.s(kw_only=True, frozen=True)
    class Props(SerializableConfig):
        size: Literal["s", "m", "l", "xl"] | None = attr.ib(default=None, metadata=skip_if_null())

    component_id = "datepicker"

    width: Width | None = attr.ib(default=None, init=False, metadata=inner())  # not supported by Datepicker
    control_props: Props | None = attr.ib(default=None, metadata=remap_skip_if_null("controlProps"))


@attr.s(kw_only=True, frozen=True)
class PlainTextRowItem(RowItem, DisplayConditionsMixin):
    component_id = "plain_text"

    text: str = attr.ib()
    hint_text: MarkdownStr | None = attr.ib(default=None, metadata=remap_skip_if_null("hintText"))


@attr.s(kw_only=True, frozen=True)
class DescriptionRowItem(RowItem, DisplayConditionsMixin):
    component_id = "description"

    text: MarkdownStr = attr.ib()


@attr.s(kw_only=True, frozen=True)
class FileInputRowItem(RowItem, FormFieldMixin, InnerFieldMixin, DisplayConditionsMixin):
    component_id = "file-input"


@attr.s(kw_only=True, frozen=True)
class StyleItem(SerializableConfig):
    width: Width | None = attr.ib(default=None, metadata=skip_if_null())


@attr.s(kw_only=True, frozen=True)
class KeyValueRowItem(RowItem, FormFieldMixin, DisplayConditionsMixin):
    component_id = "key_value"

    @attr.s(kw_only=True, frozen=True)
    class KeySelectProps(SerializableConfig):
        placeholder: str | None = attr.ib(default=None, metadata=skip_if_null())
        width: Width | None = attr.ib(default=None, metadata=skip_if_null())

    @attr.s(kw_only=True, frozen=True)
    class ValueInputProps(SerializableConfig):
        placeholder: str | None = attr.ib(default=None, metadata=skip_if_null())
        style: StyleItem | None = attr.ib(default=None, metadata=skip_if_null())
        hide_reveal_button: bool | None = attr.ib(default=None, metadata=remap_skip_if_null("hideRevealButton"))

    secret: bool | None = attr.ib(default=None, metadata=skip_if_null())
    keys: list[SelectOption] | None = attr.ib(default=None, metadata=skip_if_null())
    key_select_props: KeySelectProps | None = attr.ib(default=None, metadata=remap_skip_if_null("keySelectProps"))
    value_input_props: ValueInputProps | None = attr.ib(default=None, metadata=remap_skip_if_null("valueInputProps"))
