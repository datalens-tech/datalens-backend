import pydantic

import dl_settings


class BaseOAuthClient(dl_settings.TypedBaseSettings):
    type: str = pydantic.Field(alias="auth_type")
    conn_type: str


class AuthAPISettings(dl_settings.BaseRootSettings):
    auth_clients: dl_settings.TypedDictAnnotation[BaseOAuthClient] = pydantic.Field(default_factory=dict)
    sentry_dsn: str | None = None
