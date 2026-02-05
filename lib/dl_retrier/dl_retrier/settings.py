import pydantic

import dl_retrier.policy as policy
import dl_settings


class RetryPolicySettings(dl_settings.BaseSettings):
    total_timeout: float = policy.DEFAULT_RETRY_POLICY.total_timeout
    connect_timeout: float = policy.DEFAULT_RETRY_POLICY.connect_timeout
    request_timeout: float = policy.DEFAULT_RETRY_POLICY.request_timeout
    retries_count: int = policy.DEFAULT_RETRY_POLICY.retries_count
    retryable_codes: frozenset[int] = policy.DEFAULT_RETRY_POLICY.retryable_codes
    backoff_initial: float = policy.DEFAULT_RETRY_POLICY.backoff_initial
    backoff_factor: float = policy.DEFAULT_RETRY_POLICY.backoff_factor
    backoff_max: float = policy.DEFAULT_RETRY_POLICY.backoff_max


class RetryPolicyFactorySettings(dl_settings.BaseSettings):
    RETRY_POLICIES: dict[str, RetryPolicySettings] = pydantic.Field(default_factory=dict)
    DEFAULT_POLICY: RetryPolicySettings = pydantic.Field(default_factory=RetryPolicySettings)


__all__ = [
    "RetryPolicySettings",
    "RetryPolicyFactorySettings",
]
