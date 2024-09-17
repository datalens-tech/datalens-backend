from typing import (
    Any,
    Optional,
    Union,
)

import attr

from dl_api_connector.form_config.models.common import (
    SerializableConfig,
    TFieldName,
    Width,
    remap_skip_if_null,
    skip_if_null,
)


@attr.s(kw_only=True, frozen=True)
class FormFieldMixin:
    """Form field that can be sent to the API"""

    name: TFieldName = attr.ib()


TDisplayConditions = dict[Union[TFieldName], Any]


@attr.s(kw_only=True, frozen=True)
class DisplayConditionsMixin(SerializableConfig):
    """Allow to control item visibility based on form field values"""

    display_conditions: Optional[TDisplayConditions] = attr.ib(
        default=None, metadata=remap_skip_if_null("displayConditions")
    )


@attr.s(kw_only=True, frozen=True)
class InnerFieldMixin(SerializableConfig):
    """Inner fields are not send to the API, but can affect other fields"""

    inner: Optional[bool] = attr.ib(default=None, metadata=skip_if_null())  # false if undefined


@attr.s(kw_only=True, frozen=True)
class WidthMixin(SerializableConfig):
    width: Optional[Width] = attr.ib(default=None, metadata=skip_if_null())


class FormRow(SerializableConfig):
    pass
