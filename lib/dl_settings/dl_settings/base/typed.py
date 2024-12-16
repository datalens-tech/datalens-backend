import typing

import pydantic
import pydantic._internal._model_construction as pydantic_model_construction
import pydantic.fields

import dl_settings.base.settings as base_settings


class TypedMeta(pydantic_model_construction.ModelMetaclass):
    def __init__(cls, name: str, bases: tuple[type, ...], attrs: dict[str, typing.Any]):
        cls._classes: dict[str, type[base_settings.BaseSettings]] = {}


class TypedBaseModel(base_settings.BaseSettings, metaclass=TypedMeta):
    type: str = pydantic.Field(alias="type")

    @classmethod
    def __get_type_field_name(cls) -> str:
        alias = cls.model_fields["type"].alias

        if alias is None:
            raise ValueError("Field 'type' must have alias")

        return alias

    @classmethod
    def register(cls, name: str, class_: typing.Type) -> None:
        if name in cls._classes:
            raise ValueError(f"Class with name '{name}' already registered")

        if not issubclass(class_, cls):
            raise ValueError(f"Class '{class_}' must be subclass of '{cls}'")

        cls._classes[name] = class_

    @classmethod
    def factory(cls, data: typing.Any) -> base_settings.BaseSettings:
        if isinstance(data, base_settings.BaseSettings):
            return data

        if not isinstance(data, dict):
            raise ValueError("Data must be dict")

        class_name = data[cls.__get_type_field_name()]
        if class_name not in cls._classes:
            raise ValueError(f"Unknown type: {class_name}")

        class_ = cls._classes[class_name]

        return class_.model_validate(data)

    @classmethod
    def list_factory(cls, data: list[typing.Any]) -> typing.List[base_settings.BaseSettings]:
        if not isinstance(data, list):
            raise ValueError("Data must be sequence for list factory")

        return [cls.factory(item) for item in data]

    @classmethod
    def dict_factory(cls, data: dict[str, typing.Any]) -> typing.Dict[str, base_settings.BaseSettings]:
        if not isinstance(data, dict):
            raise ValueError("Data must be mapping for dict factory")

        import logging

        logging.error(data)

        return {key: cls.factory(value) for key, value in data.items()}


TypedBaseModelT = typing.TypeVar("TypedBaseModelT", bound=TypedBaseModel)


if typing.TYPE_CHECKING:
    TypedAnnotation = typing.Annotated[TypedBaseModelT, ...]
    TypedListAnnotation = typing.Annotated[list[TypedBaseModelT], ...]
    TypedDictAnnotation = typing.Annotated[dict[str, TypedBaseModelT], ...]
else:

    class TypedAnnotation:
        def __class_getitem__(cls, base_class: TypedBaseModelT) -> typing.Any:
            return typing.Annotated[
                pydantic.SerializeAsAny[base_class],
                pydantic.BeforeValidator(base_class.factory),
            ]

    class TypedListAnnotation:
        def __class_getitem__(cls, base_class: TypedBaseModelT) -> typing.Any:
            return typing.Annotated[
                list[pydantic.SerializeAsAny[base_class]],
                pydantic.BeforeValidator(base_class.list_factory),
            ]

    class TypedDictAnnotation:
        def __class_getitem__(cls, base_class: TypedBaseModelT) -> typing.Any:
            return typing.Annotated[
                dict[str, pydantic.SerializeAsAny[base_class]],
                pydantic.BeforeValidator(base_class.dict_factory),
            ]


__all__ = [
    "TypedBaseModel",
    "TypedAnnotation",
    "TypedListAnnotation",
    "TypedDictAnnotation",
]
