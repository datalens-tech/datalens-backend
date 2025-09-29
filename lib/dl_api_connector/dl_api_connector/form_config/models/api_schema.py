from enum import Enum
from typing import (
    Any,
    Literal,
)

import attr

from dl_api_connector.form_config.models.common import (
    AnnotationFieldName,
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
    type: Literal["string", "boolean", "object"] | None = attr.ib(default=None, metadata=skip_if_null())
    required: bool = attr.ib(default=False)
    length: int | None = attr.ib(default=None, metadata=skip_if_null())
    nullable: bool | None = attr.ib(default=None, metadata=skip_if_null())
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


ANNOTATION_API_SCHEMA_ITEMS: list[FormFieldApiSchema] = [
    FormFieldApiSchema(name=AnnotationFieldName.description, type="string", required=True, nullable=False),
]


def enrich_api_schema_with_annotation_fields(
    api_schema: FormActionApiSchema | None,
) -> FormActionApiSchema | None:
    if api_schema is None:
        return None

    existing_field_names = {item.name for item in api_schema.items}
    enriched_items = api_schema.items + [
        item for item in ANNOTATION_API_SCHEMA_ITEMS if item.name not in existing_field_names
    ]

    return FormActionApiSchema(items=enriched_items, conditions=api_schema.conditions)


@attr.s(kw_only=True, frozen=True)
class FormApiSchema(SerializableConfig):
    create: FormActionApiSchema | None = attr.ib(
        default=None,
        metadata=skip_if_null(),
        converter=enrich_api_schema_with_annotation_fields,
    )
    edit: FormActionApiSchema | None = attr.ib(
        default=None,
        metadata=skip_if_null(),
        converter=enrich_api_schema_with_annotation_fields,
    )
    check: FormActionApiSchema | None = attr.ib(default=None, metadata=skip_if_null())
