import logging
import typing
import warnings

import pydantic_settings

import dl_settings.base.settings as base_settings


LOGGER = logging.getLogger(__name__)


class WithFallbackGetAttr(base_settings.BaseRootSettings):
    """
    Mixin to add fallback to deprecated settings from dl-configs
    """

    fallback: typing.Any = None

    def __getattr__(self, item: str) -> typing.Any:
        try:
            return super().__getattr__(item)  # type: ignore # BaseSettings definitely has __getattr__, but mypy doesn't know about it
        except AttributeError:
            pass

        if self.fallback is None:
            message = f"Setting '{item}' is not found in the settings and no fallback is provided"
            LOGGER.warning(message)
            raise AttributeError(message)

        LOGGER.warning(f"Setting '{item}' is not found in the settings, trying to fallback")

        try:
            return getattr(self.fallback, item)
        except AttributeError:
            LOGGER.warning(f"Setting '{item}' is not found in the fallback using getattr")

        if item != item.upper():
            try:
                return getattr(self.fallback, item.upper())
            except AttributeError:
                LOGGER.exception(f"Setting '{item.upper()}' is not found in the fallback using getattr")

        if item != item.lower():
            try:
                return getattr(self.fallback, item.lower())
            except AttributeError:
                LOGGER.exception(f"Setting '{item.lower()}' is not found in the fallback using getattr")

        raise AttributeError(f"Setting '{item}' is not found in the settings and fallback")


class FallbackEnvSettingsSource(pydantic_settings.EnvSettingsSource):
    def __init__(
        self,
        env_keys: dict[str, str],
        settings_cls: type[pydantic_settings.BaseSettings],
        case_sensitive: bool | None = None,
        env_prefix: str | None = None,
        env_nested_delimiter: str | None = None,
        env_ignore_empty: bool | None = None,
        env_parse_none_str: str | None = None,
        env_parse_enums: bool | None = None,
    ) -> None:
        self._fallback_env_keys = env_keys
        super().__init__(
            settings_cls=settings_cls,
            case_sensitive=case_sensitive,
            env_prefix=env_prefix,
            env_nested_delimiter=env_nested_delimiter,
            env_ignore_empty=env_ignore_empty,
            env_parse_none_str=env_parse_none_str,
            env_parse_enums=env_parse_enums,
        )

    def _load_env_vars(self) -> typing.Mapping[str, str | None]:
        result = dict(super()._load_env_vars())

        for original_key, original_fallback_key in self._fallback_env_keys.items():
            key = original_key if self.case_sensitive else original_key.lower()
            fallback_key = original_fallback_key if self.case_sensitive else original_fallback_key.lower()

            if fallback_key in result and key not in result:
                warnings.warn(
                    message=f"Deprecated env var found: `{original_fallback_key}`. Use `{original_key}` instead",
                    category=DeprecationWarning,
                    stacklevel=1,
                )
                result[key] = result[fallback_key]

        return result


class WithFallbackEnvSource(base_settings.BaseRootSettings):
    """
    Mixin to add fallback to deprecated env vars. Should be redefined in the child class.
    fallback_env_keys should be a dict where:
        - keys: setting env var names
        - values: fallback env var names
    """

    fallback_env_keys: typing.ClassVar[dict[str, str]] = {}

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[pydantic_settings.BaseSettings],
        init_settings: pydantic_settings.PydanticBaseSettingsSource,
        env_settings: pydantic_settings.PydanticBaseSettingsSource,
        dotenv_settings: pydantic_settings.PydanticBaseSettingsSource,
        file_secret_settings: pydantic_settings.PydanticBaseSettingsSource,
    ) -> tuple[pydantic_settings.PydanticBaseSettingsSource, ...]:
        assert isinstance(env_settings, pydantic_settings.EnvSettingsSource)

        return super().settings_customise_sources(
            settings_cls=settings_cls,
            init_settings=init_settings,
            env_settings=FallbackEnvSettingsSource(
                settings_cls=env_settings.settings_cls,
                case_sensitive=env_settings.case_sensitive,
                env_prefix=env_settings.env_prefix,
                env_nested_delimiter=env_settings.env_nested_delimiter,
                env_ignore_empty=env_settings.env_ignore_empty,
                env_parse_none_str=env_settings.env_parse_none_str,
                env_parse_enums=env_settings.env_parse_enums,
                env_keys=cls.fallback_env_keys,
            ),
            dotenv_settings=dotenv_settings,
            file_secret_settings=file_secret_settings,
        )


__all__ = [
    "WithFallbackGetAttr",
    "WithFallbackEnvSource",
]
