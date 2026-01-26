from typing import (
    TYPE_CHECKING,
    Annotated,
    Any,
    TypeVar,
)

import pydantic

import dl_pydantic
import dl_settings.base.settings as base_settings


class TypedBaseSettings(base_settings.BaseSettings, dl_pydantic.TypedBaseModel):
    @classmethod
    def _get_class_name(cls, data: dict[str, Any]) -> str | None:
        type_key = cls.type_key()
        type_lower = None
        for key, value in data.items():
            if key.lower() == type_key.lower():
                type_lower = value.lower()
                break

        if not type_lower:
            raise ValueError(f"Data must contain '{type_key}' key")

        for registered_type in cls._classes:
            if registered_type.lower() == type_lower:
                return registered_type

        return None

    @classmethod
    def _prepare_data(cls, data: dict[str, Any]) -> dict[str, Any]:
        field_names: dict[str, str] = {}

        for field_name, field in cls.model_fields.items():
            key = field.alias or field_name
            field_names[key.lower()] = key

        return {field_names.get(key.lower(), key): value for key, value in data.items()}


TypedBaseSettingsT = TypeVar("TypedBaseSettingsT", bound=TypedBaseSettings)


if TYPE_CHECKING:
    TypedAnnotation = Annotated[TypedBaseSettingsT, ...]
    TypedListAnnotation = Annotated[list[TypedBaseSettingsT], ...]
    TypedDictAnnotation = Annotated[dict[str, TypedBaseSettingsT], ...]
    TypedDictWithTypeKeyAnnotation = Annotated[dict[str, TypedBaseSettingsT], ...]
else:

    class TypedAnnotation:
        def __class_getitem__(cls, base_class: TypedBaseSettingsT) -> Any:
            return Annotated[
                pydantic.SerializeAsAny[base_class],
                pydantic.BeforeValidator(base_class.factory),
                pydantic.SerializeAsAny(),
            ]

    class TypedListAnnotation:
        def __class_getitem__(cls, base_class: TypedBaseSettingsT) -> Any:
            return Annotated[
                list[pydantic.SerializeAsAny[base_class]],
                pydantic.BeforeValidator(base_class.list_factory),
                pydantic.SerializeAsAny(),
            ]

    class TypedDictAnnotation:
        def __class_getitem__(cls, base_class: TypedBaseSettingsT) -> Any:
            return Annotated[
                dict[str, pydantic.SerializeAsAny[base_class]],
                pydantic.BeforeValidator(base_class.dict_factory),
                pydantic.SerializeAsAny(),
            ]

    class TypedDictWithTypeKeyAnnotation:
        def __class_getitem__(cls, base_class: TypedBaseSettingsT) -> Any:
            return Annotated[
                dict[str, pydantic.SerializeAsAny[base_class]],
                pydantic.BeforeValidator(base_class.dict_with_type_key_factory),
                pydantic.SerializeAsAny(),
            ]


__all__ = [
    "TypedBaseSettings",
    "TypedAnnotation",
    "TypedListAnnotation",
    "TypedDictAnnotation",
    "TypedDictWithTypeKeyAnnotation",
]
