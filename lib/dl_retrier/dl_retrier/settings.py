import pydantic

import dl_retrier.policy as policy
import dl_settings


class RetryPolicySettings(dl_settings.BaseSettings):
    TOTAL_TIMEOUT: float = policy.DEFAULT_RETRY_POLICY.total_timeout
    CONNECT_TIMEOUT: float = policy.DEFAULT_RETRY_POLICY.connect_timeout
    REQUEST_TIMEOUT: float = policy.DEFAULT_RETRY_POLICY.request_timeout
    RETRIES_COUNT: int = policy.DEFAULT_RETRY_POLICY.retries_count
    RETRYABLE_CODES: frozenset[int] = policy.DEFAULT_RETRY_POLICY.retryable_codes
    BACKOFF_INITIAL: float = policy.DEFAULT_RETRY_POLICY.backoff_initial
    BACKOFF_FACTOR: float = policy.DEFAULT_RETRY_POLICY.backoff_factor
    BACKOFF_MAX: float = policy.DEFAULT_RETRY_POLICY.backoff_max


class RetryPolicyFactorySettings(dl_settings.BaseSettings):
    RETRY_POLICIES: dict[str, RetryPolicySettings] = pydantic.Field(default_factory=dict)
    DEFAULT_POLICY: RetryPolicySettings = pydantic.Field(default_factory=RetryPolicySettings)


__all__ = [
    "RetryPolicySettings",
    "RetryPolicyFactorySettings",
]
