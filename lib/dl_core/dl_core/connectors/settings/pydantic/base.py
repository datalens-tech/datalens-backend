import pydantic

import dl_settings


class ConnectorSettings(dl_settings.TypedBaseSettings):
    ...


# example of connector settings in app settings
# class AppSettings(dl_settings.BaseRootSettings):
#     CONNECTORS: dl_settings.TypedDictAnnotation[ConnectorSettings] = pydantic.Field(default_factory=dict)
