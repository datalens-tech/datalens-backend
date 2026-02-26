import os
from typing import ClassVar
import warnings

import pydantic
import pydantic_settings

import dl_pydantic


def _warn_extra_fields(cls: type[pydantic.BaseModel], data: dict) -> None:
    extra_field_names = set(data.keys())
    for field_name, field in cls.model_fields.items():
        real_field_name = field.alias or field_name
        extra_field_names.discard(real_field_name)

    for field_name in sorted(extra_field_names):
        warnings.warn(
            f"{cls.__name__}: extra field {field_name!r} will be ignored",
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
