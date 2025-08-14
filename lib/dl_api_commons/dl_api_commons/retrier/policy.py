import itertools
from typing import (
    Iterable,
    Optional,
    Protocol,
    Union,
)

import attr
from frozendict import frozendict

from dl_api_commons.retrier.settings import RetryPolicyFactorySettings


ErrorCode = Union[str, int]


@attr.s(frozen=True, hash=True)
class RetryPolicy:
    total_timeout: float = attr.ib()
    """
    Total timeout. Total retries time is limited by this value. If retry is attempted after this time,
    `RetrierTimeoutError` is risen.
    """

    connect_timeout: float = attr.ib()
    """
    Connection timeout. Single connection attempt is limited by this duration.
    """

    request_timeout: float = attr.ib()
    """
    Request timeout. Single attempt request duration (wait for response) is limited by this duration.
    """

    retries_count: int = attr.ib()
    """
    Total amount of retries. Total amount of attempts equals to `retries_count + 1`.
    """

    retryable_codes: frozenset[ErrorCode] = attr.ib()
    """
    Set of retryable error codes. If request error is within this set, request can be retried.
    """

    backoff_initial: float = attr.ib()
    """
    Initial delay after first failure. Used only if `retries_count >= 1`.
    """

    backoff_factor: float = attr.ib()
    """
    Backoff exponential factor. Backoff delay is computed as
    `backoff = min(backoff_max, backoff_initial * (backoff_factor ** (retry_number - 1)))`.
    """

    backoff_max: float = attr.ib()
    """
    Maximal backoff delay. Backoff delay is computed as
    `backoff = min(backoff_max, backoff_initial * (backoff_factor ** (retry_number - 1)))`.
    """

    def get_backoff_at(self, retry: int) -> float:
        return min(
            self.backoff_max,
            self.backoff_initial * (self.backoff_factor**retry),
        )

    def get_backoff(self) -> Iterable[float]:
        for idx in itertools.count():
            yield self.get_backoff_at(idx)

    def can_retry_error(self, error_code: ErrorCode) -> bool:
        return error_code in self.retryable_codes

    # TODO: Support custom allowed_methods


DEFAULT_RETRY_POLICY = RetryPolicy(
    total_timeout=10,
    connect_timeout=30,
    request_timeout=30,
    retries_count=10,
    retryable_codes=frozenset([500, 501, 502, 503, 504, 521]),
    backoff_initial=0.5,
    backoff_factor=2,
    backoff_max=120,
)


class BaseRetryPolicyFactory(Protocol):
    def get_policy(self, name: Optional[str]) -> RetryPolicy:
        """
        Get policy by specified name (case-sensitive).
        """


class RetryPolicyFactory:
    _default_policy: RetryPolicy
    _policies: frozendict[str, RetryPolicy]

    def __init__(
        self,
        settings: RetryPolicyFactorySettings,
    ):
        # Populate bucket of kitten
        if settings.DEFAULT_POLICY is not None:
            self._default_policy = RetryPolicy(
                total_timeout=settings.DEFAULT_POLICY.total_timeout,
                connect_timeout=settings.DEFAULT_POLICY.connect_timeout,
                request_timeout=settings.DEFAULT_POLICY.request_timeout,
                retries_count=settings.DEFAULT_POLICY.retries_count,
                retryable_codes=frozenset(settings.DEFAULT_POLICY.retryable_codes),
                backoff_initial=settings.DEFAULT_POLICY.backoff_initial,
                backoff_factor=settings.DEFAULT_POLICY.backoff_factor,
                backoff_max=settings.DEFAULT_POLICY.backoff_max,
            )
        else:
            self._default_policy = DEFAULT_RETRY_POLICY

        policies = {}
        for key, policy_settings in settings.RETRY_POLICIES.items():
            policies[key] = RetryPolicy(
                total_timeout=policy_settings.total_timeout,
                connect_timeout=policy_settings.connect_timeout,
                request_timeout=policy_settings.request_timeout,
                retries_count=policy_settings.retries_count,
                retryable_codes=frozenset(policy_settings.retryable_codes),
                backoff_initial=policy_settings.backoff_initial,
                backoff_factor=policy_settings.backoff_factor,
                backoff_max=policy_settings.backoff_max,
            )

        self._policies = frozendict(policies)

    def get_policy(self, name: Optional[str]) -> RetryPolicy:
        """
        Get policy by specified name. Uses settings to construct policy values. None and non-existing policy fallbacks
        to `DEFAULT_POLICY` options.
        """

        if name is None:
            return self._default_policy

        return self._policies.get(name, self._default_policy)


class DefaultRetryPolicyFactory:
    def get_policy(self, name: Optional[str]) -> RetryPolicy:
        """
        Default factory returns single pre-defined policy for every request
        """

        return DEFAULT_RETRY_POLICY
