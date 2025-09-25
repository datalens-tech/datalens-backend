from typing import (
    TYPE_CHECKING,
    Annotated,
    Any,
    TypeVar,
)

import pydantic

from dl_httpx.models import base
from dl_pydantic import TypedBaseModel


class TypedBaseSchema(TypedBaseModel, base.BaseSchema):
    ...


TypedBaseSchemaT = TypeVar("TypedBaseSchemaT", bound=TypedBaseSchema)


if TYPE_CHECKING:
    TypedSchemaAnnotation = Annotated[TypedBaseSchemaT, ...]
    TypedSchemaListAnnotation = Annotated[list[TypedBaseSchemaT], ...]
    TypedSchemaDictAnnotation = Annotated[dict[str, TypedBaseSchemaT], ...]
else:

    class TypedSchemaAnnotation:
        def __class_getitem__(cls, base_class: TypedBaseSchemaT) -> Any:
            return Annotated[
                base_class,
                pydantic.BeforeValidator(base_class.factory),
            ]

    class TypedSchemaListAnnotation:
        def __class_getitem__(cls, base_class: TypedBaseSchemaT) -> Any:
            return Annotated[
                list[base_class],
                pydantic.BeforeValidator(base_class.list_factory),
            ]

    class TypedSchemaDictAnnotation:
        def __class_getitem__(cls, base_class: TypedBaseSchemaT) -> Any:
            return Annotated[
                dict[str, base_class],
                pydantic.BeforeValidator(base_class.dict_factory),
            ]


__all__ = [
    "TypedBaseSchema",
    "TypedSchemaAnnotation",
    "TypedSchemaListAnnotation",
    "TypedSchemaDictAnnotation",
]
