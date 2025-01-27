from __future__ import annotations

from enum import Enum
from typing import (
    Any,
    Literal,
    Optional,
)

import attr

from dl_api_connector.form_config.models.common import (
    SerializableConfig,
    TFieldName,
    remap_skip_if_null,
    skip_if_null,
)


@attr.s(kw_only=True, frozen=True)
class FormFieldSelector(SerializableConfig):
    """Can be used to refer to a specific field of the form"""

    name: TFieldName = attr.ib()


class FormFieldApiAction(Enum):
    """What to do with the field when sending a request to the API (validate and include it in the request)"""

    include = "include"
    skip = "skip"


@attr.s(kw_only=True, frozen=True)
class FormFieldApiSchema(SerializableConfig):
    name: TFieldName = attr.ib()
    type: Optional[Literal["string", "boolean", "object"]] = attr.ib(default=None, metadata=skip_if_null())
    required: bool = attr.ib(default=False)
    length: Optional[int] = attr.ib(default=None, metadata=skip_if_null())
    nullable: Optional[bool] = attr.ib(default=None, metadata=skip_if_null())
    default_action: FormFieldApiAction = attr.ib(
        default=FormFieldApiAction.include, metadata=remap_skip_if_null("defaultAction")
    )


@attr.s(kw_only=True, frozen=True)
class FormFieldConditionalApiAction(SerializableConfig):
    selector: FormFieldSelector = attr.ib()
    action: FormFieldApiAction = attr.ib()


@attr.s(kw_only=True, frozen=True)
class FormFieldApiActionCondition(SerializableConfig):
    when: FormFieldSelector = attr.ib()
    equals: Any = attr.ib()
    then: list[FormFieldConditionalApiAction] = attr.ib()


@attr.s(kw_only=True, frozen=True)
class FormActionApiSchema(SerializableConfig):
    items: list[FormFieldApiSchema] = attr.ib()
    conditions: list[FormFieldApiActionCondition] = attr.ib(factory=list)


@attr.s(kw_only=True, frozen=True)
class FormApiSchema(SerializableConfig):
    create: Optional[FormActionApiSchema] = attr.ib(default=None, metadata=skip_if_null())
    edit: Optional[FormActionApiSchema] = attr.ib(default=None, metadata=skip_if_null())
    check: Optional[FormActionApiSchema] = attr.ib(default=None, metadata=skip_if_null())
