import pydantic
import pydantic_settings

import dl_settings


class BaseOAuthClient(dl_settings.TypedBaseSettings):
    type: str = pydantic.Field(alias="auth_type")
    conn_type: str


class AuthAPISettings(dl_settings.BaseRootSettings):
    model_config = pydantic_settings.SettingsConfigDict(
        case_sensitive=False,  # TODO: migrate to default
    )

    auth_clients: dl_settings.TypedDictAnnotation[BaseOAuthClient] = pydantic.Field(default_factory=dict)
    sentry_dsn: str | None = None
    obfuscation_enabled: bool = False
