import typing

import pydantic
import pydantic._internal._model_construction as pydantic_model_construction
import pydantic.fields

import dl_settings.base.settings as base_settings


class TypedMeta(pydantic_model_construction.ModelMetaclass):
    def __init__(cls, name: str, bases: tuple[type, ...], attrs: dict[str, typing.Any]):
        cls._classes: dict[str, type[base_settings.BaseSettings]] = {}


class TypedBaseSettings(base_settings.BaseSettings, metaclass=TypedMeta):
    """
    Settings class that should be used as a base for all typed settings classes.
    """

    type: str

    @classmethod
    def register(cls, name: str, class_: type) -> None:
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

        type_key = cls.model_fields["type"].alias or "type"
        if type_key not in data:
            raise ValueError(f"Data must contain '{type_key}' key")

        class_name = data[type_key]
        if class_name not in cls._classes:
            raise ValueError(f"Unknown type: {class_name}")

        class_ = cls._classes[class_name]

        # replace keys in data with field names for case-insensitive matching
        field_names: dict[str, str] = {}
        for field_name, field in class_.model_fields.items():
            key = field.alias or field_name
            field_names[key.lower()] = key

        data = {field_names.get(key.lower(), key): value for key, value in data.items()}

        return class_.model_validate(data)

    @classmethod
    def list_factory(cls, data: list[typing.Any]) -> list[base_settings.BaseSettings]:
        if not isinstance(data, list):
            raise ValueError("Data must be sequence for list factory")

        return [cls.factory(item) for item in data]

    @classmethod
    def dict_factory(cls, data: dict[str, typing.Any]) -> dict[str, base_settings.BaseSettings]:
        if not isinstance(data, dict):
            raise ValueError("Data must be mapping for dict factory")

        return {key: cls.factory(value) for key, value in data.items()}


TypedBaseSettingsT = typing.TypeVar("TypedBaseSettingsT", bound=TypedBaseSettings)


if typing.TYPE_CHECKING:
    TypedAnnotation = typing.Annotated[TypedBaseSettingsT, ...]
    TypedListAnnotation = typing.Annotated[list[TypedBaseSettingsT], ...]
    TypedDictAnnotation = typing.Annotated[dict[str, TypedBaseSettingsT], ...]
else:

    class TypedAnnotation:
        def __class_getitem__(cls, base_class: TypedBaseSettingsT) -> typing.Any:
            return typing.Annotated[
                pydantic.SerializeAsAny[base_class],
                pydantic.BeforeValidator(base_class.factory),
            ]

    class TypedListAnnotation:
        def __class_getitem__(cls, base_class: TypedBaseSettingsT) -> typing.Any:
            return typing.Annotated[
                list[pydantic.SerializeAsAny[base_class]],
                pydantic.BeforeValidator(base_class.list_factory),
            ]

    class TypedDictAnnotation:
        def __class_getitem__(cls, base_class: TypedBaseSettingsT) -> typing.Any:
            return typing.Annotated[
                dict[str, pydantic.SerializeAsAny[base_class]],
                pydantic.BeforeValidator(base_class.dict_factory),
            ]


__all__ = [
    "TypedBaseSettings",
    "TypedAnnotation",
    "TypedListAnnotation",
    "TypedDictAnnotation",
]
