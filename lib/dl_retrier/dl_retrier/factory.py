from typing import Protocol

import attr
import frozendict
import typing_extensions

import dl_retrier.policy as policy
import dl_retrier.settings as settings


class BaseRetryPolicyFactory(Protocol):
    def get_policy(self, name: str | None = None) -> policy.RetryPolicy:
        """
        Get policy by specified name (case-sensitive).
        """


@attr.s(kw_only=True, frozen=True, auto_attribs=True)
class RetryPolicyFactory:
    _default_policy: policy.RetryPolicy
    _policies: frozendict.frozendict[str, policy.RetryPolicy]

    @classmethod
    def from_settings(cls, settings: settings.RetryPolicyFactorySettings) -> typing_extensions.Self:
        policies: dict[str, policy.RetryPolicy] = {}
        for key, policy_settings in settings.RETRY_POLICIES.items():
            policies[key] = policy.RetryPolicy(
                total_timeout=policy_settings.TOTAL_TIMEOUT,
                connect_timeout=policy_settings.CONNECT_TIMEOUT,
                request_timeout=policy_settings.REQUEST_TIMEOUT,
                retries_count=policy_settings.RETRIES_COUNT,
                retryable_codes=frozenset(policy_settings.RETRYABLE_CODES),
                backoff_initial=policy_settings.BACKOFF_INITIAL,
                backoff_factor=policy_settings.BACKOFF_FACTOR,
                backoff_max=policy_settings.BACKOFF_MAX,
            )

        return cls(
            default_policy=policy.RetryPolicy(
                total_timeout=settings.DEFAULT_POLICY.TOTAL_TIMEOUT,
                connect_timeout=settings.DEFAULT_POLICY.CONNECT_TIMEOUT,
                request_timeout=settings.DEFAULT_POLICY.REQUEST_TIMEOUT,
                retries_count=settings.DEFAULT_POLICY.RETRIES_COUNT,
                retryable_codes=frozenset(settings.DEFAULT_POLICY.RETRYABLE_CODES),
                backoff_initial=settings.DEFAULT_POLICY.BACKOFF_INITIAL,
                backoff_factor=settings.DEFAULT_POLICY.BACKOFF_FACTOR,
                backoff_max=settings.DEFAULT_POLICY.BACKOFF_MAX,
            ),
            policies=frozendict.frozendict(policies),
        )

    def get_policy(self, name: str | None = None) -> policy.RetryPolicy:
        """
        Get policy by specified name. Uses settings to construct policy values. None and non-existing policy fallbacks
        to `DEFAULT_POLICY` options.
        """

        if name is None:
            return self._default_policy

        return self._policies.get(name, self._default_policy)


class DefaultRetryPolicyFactory:
    def get_policy(self, name: str | None = None) -> policy.RetryPolicy:
        """
        Default factory returns single pre-defined policy for every request
        """

        return policy.DEFAULT_RETRY_POLICY


__all__ = [
    "BaseRetryPolicyFactory",
    "RetryPolicyFactory",
    "DefaultRetryPolicyFactory",
]
