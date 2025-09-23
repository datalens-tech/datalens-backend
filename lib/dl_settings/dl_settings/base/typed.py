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
else:

    class TypedAnnotation:
        def __class_getitem__(cls, base_class: TypedBaseSettingsT) -> Any:
            return Annotated[
                pydantic.SerializeAsAny[base_class],
                pydantic.BeforeValidator(base_class.factory),
            ]

    class TypedListAnnotation:
        def __class_getitem__(cls, base_class: TypedBaseSettingsT) -> Any:
            return Annotated[
                list[pydantic.SerializeAsAny[base_class]],
                pydantic.BeforeValidator(base_class.list_factory),
            ]

    class TypedDictAnnotation:
        def __class_getitem__(cls, base_class: TypedBaseSettingsT) -> Any:
            return Annotated[
                dict[str, pydantic.SerializeAsAny[base_class]],
                pydantic.BeforeValidator(base_class.dict_factory),
            ]


__all__ = [
    "TypedBaseSettings",
    "TypedAnnotation",
    "TypedListAnnotation",
    "TypedDictAnnotation",
]
