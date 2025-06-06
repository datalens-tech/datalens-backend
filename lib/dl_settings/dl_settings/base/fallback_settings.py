import pydantic
import pydantic_settings

import dl_settings.base.fallback as base_fallback
import dl_settings.base.settings as base_settings


class BaseRootSettingsWithFallback(
    base_fallback.WithFallbackGetAttr,
    base_fallback.WithFallbackEnvSource,
    base_settings.BaseRootSettings,
):
    # Moved here, see https://github.com/pydantic/pydantic/issues/9992
    model_config = pydantic_settings.SettingsConfigDict(
        extra=pydantic.Extra.ignore,
    )


__all__ = [
    "BaseRootSettingsWithFallback",
]
