from typing import ClassVar

import dl_settings


class ConnectorSettings(dl_settings.TypedBaseSettings):
    pydantic_env_fallback: ClassVar[dict[str, str]] = {}


# example of connector settings in app settings
# class AppSettings(dl_settings.BaseRootSettings):
#     CONNECTORS: dl_settings.TypedDictWithTypeKeyAnnotation[ConnectorSettings] = pydantic.Field(default_factory=dict)
