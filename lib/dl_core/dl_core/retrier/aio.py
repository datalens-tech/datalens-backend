import asyncio
import logging
import time
from typing import (
    Any,
    Awaitable,
    Callable,
)

import aiohttp
import aiohttp.client_exceptions
from aiohttp.client_exceptions import ClientError
import attr

from dl_api_commons.aiohttp.aiohttp_client import BaseRetrier

from .policy import RetryPolicy


LOGGER = logging.getLogger(__name__)


class AIORetryTimeout(aiohttp.client_exceptions.ServerTimeoutError):
    """Timed out attempting to retry"""


@attr.s(kw_only=True, frozen=True)
class AIOPolicyRetrier(BaseRetrier):
    """
    Retrier for AIO client
    """

    _retry_policy: RetryPolicy = attr.ib()

    # TODO: Merge with requests.RequestsPolicyRetrier as sync/async-independent implementation

    async def retry_request(
        self,
        req_func: Callable[..., Awaitable[aiohttp.ClientResponse]],
        method: str,
        *args: Any,
        **kwargs: Any,
    ) -> aiohttp.ClientResponse:
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
                        read_timeout_sec=min(rest_dt, self._retry_policy.request_timeout),
                        conn_timeout_sec=min(rest_dt, self._retry_policy.connect_timeout),
                    )
                )

                resp = await req_func(method, *args, **kwargs)
            except ClientError as err:
                LOGGER.warning("aiohttp client error: %r", err)

                if is_last:
                    raise err
            else:
                if self._retry_policy.can_retry_error(resp.status):
                    LOGGER.warning("HTTP error: %r, ", resp.status)

                if not self._retry_policy.can_retry_error(resp.status) or is_last:
                    return resp

            # Check if we still have time to sleep
            last_ts = time.time()
            rest_dt = self._retry_policy.total_timeout - (last_ts - start_ts)
            next_backoff = self._retry_policy.get_backoff_at(idx)

            # Can't last backoff must fit into the total timeout
            if rest_dt <= next_backoff:
                raise AIORetryTimeout()

            await asyncio.sleep(next_backoff)

        raise Exception("You should not be here")
