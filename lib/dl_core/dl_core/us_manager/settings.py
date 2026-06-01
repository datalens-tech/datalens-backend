from typing import Annotated

import pydantic

import dl_retrier
import dl_settings


class USClientSettings(dl_settings.BaseSettings):
    RETRY_POLICY: dl_retrier.RetryPolicyFactorySettings = pydantic.Field(
        default_factory=dl_retrier.RetryPolicyFactorySettings
    )
    MASTER_TOKEN_AUTHORIZATION_ENABLED: bool = True
    DYNAMIC_AUTH_PRIVATE_KEY: Annotated[str, dl_settings.decode_multiline_validator] | None = pydantic.Field(
        default=None,
        repr=False,
    )
    DYNAMIC_AUTH_TOKEN_LIFETIME_SEC: int = 3600
    DYNAMIC_AUTH_MIN_TTL_SEC: float = 900.0
