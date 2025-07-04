import datetime
from typing import Iterator

import attr

from dl_core.retrier.policy import RetryPolicy


@attr.s(kw_only=True, frozen=True, auto_attribs=True)
class Retry:
    request_timeout: float
    connect_timeout: float
    sleep_before_seconds: float


def iter_retries(retry_policy: RetryPolicy) -> Iterator[Retry]:
    start_dt = datetime.datetime.now()
    end_dt = start_dt + datetime.timedelta(seconds=retry_policy.total_timeout)

    for idx in range(retry_policy.retries_count + 1):  # First Retry is not a retry
        sleep_before_seconds = 0 if idx == 0 else retry_policy.get_backoff_at(idx - 1)
        total_timeout_remaining = (end_dt - datetime.datetime.now()).total_seconds()

        if sleep_before_seconds > total_timeout_remaining:
            break

        total_timeout_remaining -= sleep_before_seconds

        yield Retry(
            request_timeout=min(retry_policy.request_timeout, total_timeout_remaining),
            connect_timeout=min(retry_policy.connect_timeout, total_timeout_remaining),
            sleep_before_seconds=sleep_before_seconds,
        )
