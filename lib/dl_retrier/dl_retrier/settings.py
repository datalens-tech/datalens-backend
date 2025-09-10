import pydantic

import dl_settings


class RetryPolicySettings(dl_settings.BaseSettings):
    total_timeout: float = 120
    connect_timeout: float = 30
    request_timeout: float = 30
    retries_count: int = 10
    retryable_codes: set[int] = pydantic.Field(default_factory=lambda: set([500, 501, 502, 503, 504, 521]))
    backoff_initial: float = 0.5
    backoff_factor: float = 2.0
    backoff_max: float = 120.0


class RetryPolicyFactorySettings(dl_settings.BaseSettings):
    RETRY_POLICIES: dict[str, RetryPolicySettings] = pydantic.Field(default_factory=dict)
    DEFAULT_POLICY: RetryPolicySettings = pydantic.Field(default_factory=RetryPolicySettings)


__all__ = [
    "RetryPolicySettings",
    "RetryPolicyFactorySettings",
]
