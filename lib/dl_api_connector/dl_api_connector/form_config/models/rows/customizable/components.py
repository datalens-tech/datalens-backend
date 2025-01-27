from typing import (
    Literal,
    Optional,
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
    align: Optional[Align] = attr.ib(default=None, metadata=skip_if_null())
    help_text: Optional[MarkdownStr] = attr.ib(default=None, metadata=remap_skip_if_null("helpText"))


@attr.s(kw_only=True, frozen=True)
class SelectableOption:  # TODO rename to RadioButtonOption
    text: str = attr.ib()
    value: str = attr.ib()


@attr.s(kw_only=True, frozen=True)
class InputRowItem(ControlRowItem, PlaceholderMixin):
    @attr.s(kw_only=True, frozen=True)
    class Props(SerializableConfig):
        multiline: Optional[bool] = attr.ib(default=None, metadata=skip_if_null())  # false if undefined
        type: Optional[Literal["text", "password", "number"]] = attr.ib(default="text", metadata=skip_if_null())
        disabled: Optional[bool] = attr.ib(default=None, metadata=skip_if_null())

    component_id = "input"

    control_props: Optional[Props] = attr.ib(default=None, metadata=remap_skip_if_null("controlProps"))


@attr.s(kw_only=True, frozen=True)
class SelectOption(SerializableConfig):
    class Data(SerializableConfig):
        description: Optional[str] = attr.ib(default=None, metadata=skip_if_null())

    content: str = attr.ib()
    value: str = attr.ib()
    data: Optional[Data] = attr.ib(default=None, metadata=skip_if_null())


@attr.s(kw_only=True, frozen=True)
class SelectRowItem(ControlRowItem, PlaceholderMixin):
    @attr.s(kw_only=True, frozen=True)
    class Props(SerializableConfig):
        show_search: Optional[bool] = attr.ib(default=None, metadata=remap_skip_if_null("showSearch"))
        has_clear: Optional[bool] = attr.ib(default=None, metadata=remap_skip_if_null("hasClear"))
        multiple: Optional[bool] = attr.ib(default=None, metadata=skip_if_null())

    component_id = "select"

    available_values: Optional[list[SelectOption]] = attr.ib(
        default=None, metadata=remap_skip_if_null("availableValues")
    )
    control_props: Optional[Props] = attr.ib(default=None, metadata=remap_skip_if_null("controlProps"))


@attr.s(kw_only=True, frozen=True)
class RadioButtonRowItem(ControlRowItem):
    component_id = "radio_button"

    options: list[SelectableOption] = attr.ib()


@attr.s(kw_only=True, frozen=True)
class RadioGroupRowItemOption(SerializableConfig):
    @attr.s(kw_only=True, frozen=True)
    class ValueContent(SerializableConfig):
        text: str = attr.ib()
        hint_text: Optional[MarkdownStr] = attr.ib(default=None, metadata=remap_skip_if_null("hintText"))

    content: ValueContent = attr.ib()
    value: str = attr.ib()


@attr.s(kw_only=True, frozen=True)
class RadioGroupRowItem(RowItem, DisplayConditionsMixin, FormFieldMixin, InnerFieldMixin):
    @attr.s(kw_only=True, frozen=True)
    class Props(SerializableConfig):
        disabled: Optional[bool] = attr.ib(default=None, metadata=skip_if_null())

    component_id = "radio_group"

    options: list[RadioGroupRowItemOption] = attr.ib()
    default_value: Optional[str] = attr.ib(default=None, metadata=remap_skip_if_null("defaultValue"))
    control_props: Optional[Props] = attr.ib(default=None, metadata=remap_skip_if_null("controlProps"))

    def __attrs_post_init__(self) -> None:
        if self.default_value is not None and self.default_value not in (
            possible_values := [option.value for option in self.options]
        ):
            raise ValueError(f"Invalid default value for radio group: {self.default_value} is not in {possible_values}")


@attr.s(kw_only=True, frozen=True)
class CheckboxRowItem(RowItem, DisplayConditionsMixin, FormFieldMixin, InnerFieldMixin, DefaultValueMixin):
    @attr.s(kw_only=True, frozen=True)
    class Props(SerializableConfig):
        size: Optional[Literal["m", "l"]] = attr.ib(default=None, metadata=skip_if_null())
        qa: Optional[str] = attr.ib(
            default=None, metadata=skip_if_null()
        )  # UI-specific testing stuff, should be removed at some point

    component_id = "checkbox"

    text: str = attr.ib()
    control_props: Optional[Props] = attr.ib(default=None, metadata=remap_skip_if_null("controlProps"))


@attr.s(kw_only=True, frozen=True)
class DatepickerRowItem(ControlRowItem):
    @attr.s(kw_only=True, frozen=True)
    class Props(SerializableConfig):
        size: Optional[Literal["s", "m", "l", "xl"]] = attr.ib(default=None, metadata=skip_if_null())

    component_id = "datepicker"

    width: Optional[Width] = attr.ib(default=None, init=False, metadata=inner())  # not supported by Datepicker
    control_props: Optional[Props] = attr.ib(default=None, metadata=remap_skip_if_null("controlProps"))


@attr.s(kw_only=True, frozen=True)
class PlainTextRowItem(RowItem, DisplayConditionsMixin):
    component_id = "plain_text"

    text: str = attr.ib()
    hint_text: Optional[MarkdownStr] = attr.ib(default=None, metadata=remap_skip_if_null("hintText"))


@attr.s(kw_only=True, frozen=True)
class DescriptionRowItem(RowItem, DisplayConditionsMixin):
    component_id = "description"

    text: MarkdownStr = attr.ib()


@attr.s(kw_only=True, frozen=True)
class FileInputRowItem(RowItem, FormFieldMixin, InnerFieldMixin, DisplayConditionsMixin):
    component_id = "file-input"


@attr.s(kw_only=True, frozen=True)
class StyleItem(SerializableConfig):
    width: Optional[Width] = attr.ib(default=None, metadata=skip_if_null())


@attr.s(kw_only=True, frozen=True)
class KeyValueRowItem(RowItem, FormFieldMixin, DisplayConditionsMixin):
    component_id = "key_value"

    @attr.s(kw_only=True, frozen=True)
    class KeySelectProps(SerializableConfig):
        placeholder: Optional[str] = attr.ib(default=None, metadata=skip_if_null())
        width: Optional[Width] = attr.ib(default=None, metadata=skip_if_null())

    @attr.s(kw_only=True, frozen=True)
    class ValueInputProps(SerializableConfig):
        placeholder: Optional[str] = attr.ib(default=None, metadata=skip_if_null())
        style: Optional[StyleItem] = attr.ib(default=None, metadata=skip_if_null())
        hide_reveal_button: Optional[bool] = attr.ib(default=None, metadata=skip_if_null())

    secret: Optional[bool] = attr.ib(default=None, metadata=skip_if_null())
    keys: Optional[list[SelectOption]] = attr.ib(default=None, metadata=skip_if_null())
    key_select_props: Optional[KeySelectProps] = attr.ib(default=None, metadata=remap_skip_if_null("keySelectProps"))
    value_input_props: Optional[ValueInputProps] = attr.ib(default=None, metadata=remap_skip_if_null("valueInputProps"))
