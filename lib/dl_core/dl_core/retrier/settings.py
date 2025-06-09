import dl_settings


# Example:
# US_CLIENT:
#   RETRY_POLICY:
#     DEFAULT_POLICY:
#       total_timeout: 30
#       total_retries: 3
#     RETRY_POLICIES:
#       GET_ENTITY:
#         connect_timeout: 1
#         request_timeout: 1
#         backoff_factor: 10
#
# When no key found in `RETRY_POLICIES`, policy from `DEFAULT_POLICY` is used.
# If no field set in `DEFAULT_POLICY`, it defaults to field in settings class.


class RetryPolicySettings(dl_settings.BaseSettings):
    total_timeout: float = 120
    connect_timeout: float = 30
    request_timeout: float = 30
    retries_count: int = 10
    retryable_codes: set[int] = set([500, 501, 502, 503, 504, 521])
    backoff_initial: float = 0.5
    backoff_factor: float = 2.0
    backoff_max: float = 120.0


class RetryPolicyFactorySettings(dl_settings.BaseSettings):
    RETRY_POLICIES: dict[str, RetryPolicySettings] = {}
    DEFAULT_POLICY: RetryPolicySettings = RetryPolicySettings()
