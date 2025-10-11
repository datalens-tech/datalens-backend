from typing import (
    Any,
    ClassVar,
    Optional,
)

import attr

from dl_api_connector.form_config.models.common import (
    MarkdownStr,
    SerializableConfig,
    remap,
)
from dl_api_connector.form_config.models.rows.base import (
    DisplayConditionsMixin,
    FormFieldMixin,
    FormRow,
    InnerFieldMixin,
    WidthMixin,
)


@attr.s(kw_only=True, frozen=True)
class RowItem(SerializableConfig):
    component_id: ClassVar[str]

    def as_dict(self) -> dict[str, Any]:
        return dict(
            id=self.component_id,
            **super().as_dict(),
        )


@attr.s(kw_only=True, frozen=True)
class PlaceholderMixin(SerializableConfig):
    placeholder: Optional[str] = attr.ib(default=None)


@attr.s(kw_only=True, frozen=True)
class DefaultValueMixin(SerializableConfig):
    # comma separated if multiple=true
    default_value: Optional[str | bool] = attr.ib(default=None, metadata=remap("defaultValue"))


@attr.s(kw_only=True, frozen=True)
class ControlRowItem(RowItem, FormFieldMixin, DisplayConditionsMixin, InnerFieldMixin, DefaultValueMixin, WidthMixin):
    fake_value: Optional[str] = attr.ib(default=None, metadata=remap("fakeValue"))
    hint_text: Optional[MarkdownStr] = attr.ib(default=None, metadata=remap("hintText"))


@attr.s(kw_only=True, frozen=True)
class CustomizableRow(FormRow):
    items: list[RowItem] = attr.ib()
