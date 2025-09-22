from typing import (
    TYPE_CHECKING,
    Annotated,
    Any,
    TypeVar,
)

import pydantic

from dl_pydantic import TypedBaseModel

from .base import BaseResponseModel


class TypedBaseResponseModel(TypedBaseModel, BaseResponseModel):
    ...


TypedBaseResponseModelT = TypeVar("TypedBaseResponseModelT", bound=TypedBaseResponseModel)


if TYPE_CHECKING:
    TypedResponseAnnotation = Annotated[TypedBaseResponseModelT, ...]
    TypedResponseListAnnotation = Annotated[list[TypedBaseResponseModelT], ...]
    TypedResponseDictAnnotation = Annotated[dict[str, TypedBaseResponseModelT], ...]
else:

    class TypedResponseAnnotation:
        def __class_getitem__(cls, base_class: TypedBaseResponseModelT) -> Any:
            return Annotated[
                base_class,
                pydantic.BeforeValidator(base_class.factory),
            ]

    class TypedResponseListAnnotation:
        def __class_getitem__(cls, base_class: TypedBaseResponseModelT) -> Any:
            return Annotated[
                list[base_class],
                pydantic.BeforeValidator(base_class.list_factory),
            ]

    class TypedResponseDictAnnotation:
        def __class_getitem__(cls, base_class: TypedBaseResponseModelT) -> Any:
            return Annotated[
                dict[str, base_class],
                pydantic.BeforeValidator(base_class.dict_factory),
            ]


__all__ = [
    "TypedBaseResponseModel",
    "TypedResponseAnnotation",
    "TypedResponseListAnnotation",
    "TypedResponseDictAnnotation",
]
