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
                total_timeout=policy_settings.total_timeout,
                connect_timeout=policy_settings.connect_timeout,
                request_timeout=policy_settings.request_timeout,
                retries_count=policy_settings.retries_count,
                retryable_codes=frozenset(policy_settings.retryable_codes),
                backoff_initial=policy_settings.backoff_initial,
                backoff_factor=policy_settings.backoff_factor,
                backoff_max=policy_settings.backoff_max,
            )

        return cls(
            default_policy=policy.RetryPolicy(
                total_timeout=settings.DEFAULT_POLICY.total_timeout,
                connect_timeout=settings.DEFAULT_POLICY.connect_timeout,
                request_timeout=settings.DEFAULT_POLICY.request_timeout,
                retries_count=settings.DEFAULT_POLICY.retries_count,
                retryable_codes=frozenset(settings.DEFAULT_POLICY.retryable_codes),
                backoff_initial=settings.DEFAULT_POLICY.backoff_initial,
                backoff_factor=settings.DEFAULT_POLICY.backoff_factor,
                backoff_max=settings.DEFAULT_POLICY.backoff_max,
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
