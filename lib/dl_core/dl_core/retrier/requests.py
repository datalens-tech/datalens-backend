import logging
import time
from typing import Callable

import attr
import requests
import requests.adapters

from .policy import RetryPolicy


LOGGER = logging.getLogger(__name__)


class RequestsRetryTimeout(requests.exceptions.Timeout):
    """Timed out attempting to retry"""


@attr.s(kw_only=True, frozen=True)
class RequestsPolicyRetrier:
    """
    Retrier for requests session
    """

    _retry_policy: RetryPolicy = attr.ib()

    # TODO: Merge with aio.AIOPolicyRetrier as sync/async-independent implementation

    def retry_request(
        self,
        req_func: Callable[..., requests.Response],
        *args,
        **kwargs,
    ) -> requests.Response:
        start_ts = time.time()
        rest_dt = self._retry_policy.total_timeout

        for idx in range(self._retry_policy.retries_count):
            is_last = idx == (self._retry_policy.retries_count - 1)

            try:
                # Limit request time to the rest of total timeout
                last_ts = time.time()
                rest_dt = self._retry_policy.total_timeout - (last_ts - start_ts)

                kwargs.update(
                    dict(
                        timeout=(
                            min(rest_dt, self._retry_policy.connect_timeout),
                            min(rest_dt, self._retry_policy.request_timeout),
                        ),
                    )
                )

                resp = req_func(*args, **kwargs)
            except requests.exceptions.RequestException as err:
                LOGGER.warning("requests client error: %r", err)

                if is_last:
                    raise err
            else:
                if self._retry_policy.can_retry_error(resp.status_code):
                    LOGGER.warning("HTTP error: %r, ", resp.status_code)

                if not self._retry_policy.can_retry_error(resp.status_code) or is_last:
                    return resp

            # Check if we still have time to sleep
            last_ts = time.time()
            rest_dt = self._retry_policy.total_timeout - (last_ts - start_ts)
            next_backoff = self._retry_policy.get_backoff_at(idx)

            # Can't last backoff must fit into the total timeout
            if rest_dt <= next_backoff:
                raise RequestsRetryTimeout()

            time.sleep(next_backoff)

        raise Exception("You should not be here")
