import logging
from typing import (
    TYPE_CHECKING,
    Annotated,
    Any,
    Type,
    TypeVar,
    cast,
)

import pydantic
import pydantic._internal._model_construction as pydantic_model_construction
from typing_extensions import Self

import dl_pydantic.base as base


TypedBaseModelT = TypeVar("TypedBaseModelT", bound="TypedBaseModel")


class TypedMeta(pydantic_model_construction.ModelMetaclass):
    def __init__(cls, name: str, bases: tuple[type, ...], attrs: dict[str, Any]):
        cls._classes: dict[str, type["TypedBaseModel"]] = {}


class TypedBaseModel(base.BaseModel, metaclass=TypedMeta):
    """
    Settings class that should be used as a base for all typed settings classes.
    """

    type: str

    @classmethod
    def register(cls, name: str, class_: Type) -> None:  # noqa: UP006
        if name in cls._classes:
            raise ValueError(f"Class with name '{name}' already registered")

        if not issubclass(class_, cls):
            raise ValueError(f"Class '{class_}' must be subclass of '{cls}'")

        cls._classes[name] = class_
        logging.info(f"Registered class '{name}' as '{class_}'")

    @classmethod
    def _prepare_data(cls, data: dict[str, Any]) -> dict[str, Any]:
        return data

    @classmethod
    def _get_class_name(cls, data: dict[str, Any]) -> str:
        type_key = cls.model_fields["type"].alias or "type"
        if type_key not in data:
            raise ValueError(f"Data must contain '{type_key}' key")

        return data[type_key]

    @classmethod
    def factory(cls, data: Any) -> Self:
        if isinstance(data, cls):
            return data

        if not isinstance(data, dict):
            raise ValueError("Data must be dict")

        class_name = cls._get_class_name(data)
        if class_name not in cls._classes:
            raise ValueError(f"Unknown type: {class_name}")
        class_ = cls._classes[class_name]

        data = class_._prepare_data(data)

        return cast(Self, class_.model_validate(data))

    @classmethod
    def list_factory(cls, data: list[Any]) -> list[base.BaseModel]:
        if not isinstance(data, list):
            raise ValueError("Data must be sequence for list factory")

        return [cls.factory(item) for item in data]

    @classmethod
    def dict_factory(cls, data: dict[str, Any]) -> dict[str, base.BaseModel]:
        if not isinstance(data, dict):
            raise ValueError("Data must be mapping for dict factory")

        return {key: cls.factory(value) for key, value in data.items()}


if TYPE_CHECKING:
    TypedAnnotation = Annotated[TypedBaseModelT, ...]
    TypedListAnnotation = Annotated[list[TypedBaseModelT], ...]
    TypedDictAnnotation = Annotated[dict[str, TypedBaseModelT], ...]
else:

    class TypedAnnotation:
        def __class_getitem__(cls, base_class: TypedBaseModelT) -> Any:
            return Annotated[
                pydantic.SerializeAsAny[base_class],
                pydantic.BeforeValidator(base_class.factory),
            ]

    class TypedListAnnotation:
        def __class_getitem__(cls, base_class: TypedBaseModelT) -> Any:
            return Annotated[
                list[pydantic.SerializeAsAny[base_class]],
                pydantic.BeforeValidator(base_class.list_factory),
            ]

    class TypedDictAnnotation:
        def __class_getitem__(cls, base_class: TypedBaseModelT) -> Any:
            return Annotated[
                dict[str, pydantic.SerializeAsAny[base_class]],
                pydantic.BeforeValidator(base_class.dict_factory),
            ]


__all__ = [
    "TypedBaseModel",
    "TypedAnnotation",
    "TypedListAnnotation",
    "TypedDictAnnotation",
]
