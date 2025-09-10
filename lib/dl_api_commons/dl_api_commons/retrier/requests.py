import logging
import time
from typing import (
    Any,
    Callable,
)

import attr
import requests
import requests.adapters

import dl_retrier


LOGGER = logging.getLogger(__name__)


class RequestsRetryError(Exception):
    """Requests retrier failed attempting to retry"""


class RequestsRetryTimeout(requests.exceptions.Timeout, RequestsRetryError):
    """Timed out attempting to retry"""


@attr.s(kw_only=True, frozen=True)
class RequestsPolicyRetrier:
    """
    Retrier for requests session
    """

    _retry_policy: dl_retrier.RetryPolicy = attr.ib()

    # TODO: Merge with aio.AiohttpPolicyRetrier as sync/async-independent implementation

    def retry_request(
        self,
        req_func: Callable[..., requests.Response],
        *args: Any,
        **kwargs: Any,
    ) -> requests.Response:
        last_known_result: requests.Response | Exception | None = None

        for retry in self._retry_policy.iter_retries():
            if retry.sleep_before_seconds > 0:
                time.sleep(retry.sleep_before_seconds)

            kwargs["timeout"] = (retry.connect_timeout, retry.request_timeout)

            try:
                resp = req_func(*args, **kwargs)
            except requests.exceptions.RequestException as err:
                LOGGER.warning("requests client error", exc_info=True)
                last_known_result = err
                continue

            if not self._retry_policy.can_retry_error(resp.status_code):
                return resp

            last_known_result = resp

        if isinstance(last_known_result, requests.Response):
            return last_known_result

        if isinstance(last_known_result, requests.Timeout):
            raise RequestsRetryTimeout() from last_known_result

        if isinstance(last_known_result, Exception):
            raise RequestsRetryError() from last_known_result

        raise RequestsRetryError("Not a single retry was fired")
