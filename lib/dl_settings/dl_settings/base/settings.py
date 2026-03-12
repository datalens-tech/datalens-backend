import collections.abc
import os
from typing import (
    Any,
    ClassVar,
)
import warnings

import pydantic
import pydantic_settings

import dl_pydantic


def _formatted_repr(value: Any, indent: int) -> str:
    child_prefix = "  " * (indent + 1)

    if isinstance(value, pydantic.BaseModel):
        result = f"{type(value).__name__}:"
        for k, v in value.__repr_args__():
            result += f"\n{child_prefix}{k}: {_formatted_repr(v, indent + 1)}"
        return result

    if isinstance(value, (list, tuple)):
        if not value:
            return "[]"
        result = ""
        for item in value:
            result += f"\n{child_prefix}- {_formatted_repr(item, indent + 1)}"
        return result

    if isinstance(value, (set, frozenset)):
        if not value:
            return repr(value)
        result = ""
        for item in sorted(value, key=repr):
            result += f"\n{child_prefix}- {_formatted_repr(item, indent + 1)}"
        return result

    if isinstance(value, collections.abc.Mapping):
        if not value:
            return "{}"
        result = ""
        for k, v in value.items():
            result += f"\n{child_prefix}{k}: {_formatted_repr(v, indent + 1)}"
        return result

    return repr(value)


def _warn_extra_fields(cls: type[pydantic.BaseModel], data: dict) -> None:
    extra_field_names = set(data.keys())
    for field_name, field in cls.model_fields.items():
        real_field_name = field.alias or field_name
        extra_field_names.discard(real_field_name)

    for field_name in sorted(extra_field_names):
        warnings.warn(
            f"{cls.__module__}.{cls.__qualname__}: extra field {field_name!r} will be ignored",
            UserWarning,
            stacklevel=2,
        )


class BaseSettings(dl_pydantic.BaseModel):
    """
    Base settings class that should be used for sub-models and mixins.
    """

    MODEL_ENABLE_EXTRA_FIELDS_WARNING: ClassVar[bool] = True

    @pydantic.model_validator(mode="before")
    @classmethod
    def _warn_extra_fields(cls, data: dict) -> dict:
        if isinstance(data, dict) and cls.MODEL_ENABLE_EXTRA_FIELDS_WARNING:
            _warn_extra_fields(cls, data)

        return data


class BaseRootSettings(pydantic_settings.BaseSettings):
    """
    Base settings class that should be used for the root settings model.
    """

    model_config = pydantic_settings.SettingsConfigDict(
        env_nested_delimiter="__",
        extra="ignore",
        case_sensitive=True,
        hide_input_in_errors=True,
    )
    MODEL_ENABLE_EXTRA_FIELDS_WARNING: ClassVar[bool] = True

    @pydantic.model_validator(mode="before")
    @classmethod
    def _warn_extra_fields(cls, data: dict) -> dict:
        if isinstance(data, dict) and cls.MODEL_ENABLE_EXTRA_FIELDS_WARNING:
            _warn_extra_fields(cls, data)

        return data

    @classmethod
    def _get_yaml_source_paths(cls) -> list[str]:
        config_paths = os.environ.get("CONFIG_PATH", None)

        if not config_paths:
            return []

        return [path.strip() for path in config_paths.split(",")]

    @classmethod
    def _get_yaml_sources(cls) -> list[pydantic_settings.YamlConfigSettingsSource]:
        return [
            pydantic_settings.YamlConfigSettingsSource(
                cls,
                yaml_file=yaml_file,
            )
            for yaml_file in cls._get_yaml_source_paths()
        ]

    def model_formatted_repr(self) -> str:
        return _formatted_repr(self, indent=0)

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[pydantic_settings.BaseSettings],
        init_settings: pydantic_settings.PydanticBaseSettingsSource,
        env_settings: pydantic_settings.PydanticBaseSettingsSource,
        dotenv_settings: pydantic_settings.PydanticBaseSettingsSource,
        file_secret_settings: pydantic_settings.PydanticBaseSettingsSource,
    ) -> tuple[pydantic_settings.PydanticBaseSettingsSource, ...]:
        return (
            env_settings,
            *cls._get_yaml_sources(),
            dotenv_settings,
            file_secret_settings,
            init_settings,
        )


__all__ = [
    "BaseRootSettings",
    "BaseSettings",
]
