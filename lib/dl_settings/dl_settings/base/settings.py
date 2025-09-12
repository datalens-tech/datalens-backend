import os

import pydantic_settings

import dl_pydantic


class BaseSettings(dl_pydantic.BaseModel):
    """
    Base settings class that should be used for sub-models and mixins.
    """

    ...


class BaseRootSettings(pydantic_settings.BaseSettings):
    """
    Base settings class that should be used for the root settings model.
    """

    model_config = pydantic_settings.SettingsConfigDict(
        env_nested_delimiter="__",
    )

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
    "BaseSettings",
    "BaseRootSettings",
]
