from typing import ClassVar

import dl_settings


class ConnectorSettings(dl_settings.TypedBaseSettings):
    root_fallback_env_keys: ClassVar[dict[str, str]] = {}


# example of connector settings in app settings
# class AppSettings(dl_settings.BaseRootSettings):
#     CONNECTORS: dl_settings.TypedDictWithTypeKeyAnnotation[ConnectorSettings] = pydantic.Field(default_factory=dict)
