import datetime
import http
from typing import (
    Iterator,
    TypeAlias,
)

import attr


@attr.s(kw_only=True, frozen=True, auto_attribs=True)
class Retry:
    attempt_number: int
    request_timeout: float
    connect_timeout: float
    sleep_before_seconds: float


ErrorCode: TypeAlias = int


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
    `backoff = min(backoff_max, backoff_initial * (backoff_factor ** (attempt_number - 2)))`.
    """

    backoff_max: float = attr.ib()
    """
    Maximal backoff delay. Backoff delay is computed as
    `backoff = min(backoff_max, backoff_initial * (backoff_factor ** (attempt_number - 2)))`.
    """

    def _get_sleep_before(self, attempt_number: int) -> float:
        if attempt_number <= 1:
            return 0

        return min(
            self.backoff_max,
            self.backoff_initial * (self.backoff_factor ** (attempt_number - 2)),
        )

    def can_retry_error(self, error_code: ErrorCode) -> bool:
        return error_code in self.retryable_codes

    def iter_retries(self) -> Iterator[Retry]:
        start_dt = datetime.datetime.now()
        end_dt = start_dt + datetime.timedelta(seconds=self.total_timeout)
        max_attempts_count = self.retries_count + 1  # First attempt is not a retry

        for attempt_number in range(1, max_attempts_count + 1):
            sleep_before_seconds = self._get_sleep_before(attempt_number)
            total_timeout_remaining = (end_dt - datetime.datetime.now()).total_seconds()

            if sleep_before_seconds > total_timeout_remaining:
                break

            total_timeout_remaining -= sleep_before_seconds

            yield Retry(
                attempt_number=attempt_number,
                request_timeout=min(self.request_timeout, total_timeout_remaining),
                connect_timeout=min(self.connect_timeout, total_timeout_remaining),
                sleep_before_seconds=sleep_before_seconds,
            )


DEFAULT_RETRY_POLICY: RetryPolicy = RetryPolicy(
    total_timeout=120,
    connect_timeout=30,
    request_timeout=30,
    retries_count=10,
    retryable_codes=frozenset(
        [
            http.HTTPStatus.TOO_MANY_REQUESTS,
            http.HTTPStatus.INTERNAL_SERVER_ERROR,
            http.HTTPStatus.NOT_IMPLEMENTED,
            http.HTTPStatus.BAD_GATEWAY,
            http.HTTPStatus.SERVICE_UNAVAILABLE,
            http.HTTPStatus.GATEWAY_TIMEOUT,
            521,  # Web Server Is Down
        ]
    ),
    backoff_initial=0.5,
    backoff_factor=2,
    backoff_max=120,
)


__all__ = [
    "RetryPolicy",
    "DEFAULT_RETRY_POLICY",
]
