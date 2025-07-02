import asyncio
import logging
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
from dl_core.retrier.policy import RetryPolicy
from dl_core.retrier.retries import iter_retries


LOGGER = logging.getLogger(__name__)


class AiohttpRetryTimeout(aiohttp.client_exceptions.ServerTimeoutError):
    """Timed out attempting to retry"""


@attr.s(kw_only=True, frozen=True)
class AiohttpPolicyRetrier(BaseRetrier):
    """
    Retrier for Aiohttp client
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
        last_known_result: Exception | aiohttp.ClientResponse | None = None

        for retry in iter_retries(self._retry_policy):
            if retry.sleep_before_seconds > 0:
                await asyncio.sleep(retry.sleep_before_seconds)

            kwargs["read_timeout_sec"] = retry.request_timeout
            kwargs["conn_timeout_sec"] = retry.connect_timeout

            try:
                resp = await req_func(method, *args, **kwargs)
            except ClientError as err:
                LOGGER.warning("aiohttp client error", exc_info=True)
                last_known_result = err

            if not self._retry_policy.can_retry_error(resp.status):
                return resp

            last_known_result = resp

        if isinstance(last_known_result, Exception):
            raise AiohttpRetryTimeout from last_known_result

        if isinstance(last_known_result, aiohttp.ClientResponse):
            return last_known_result

        raise AiohttpRetryTimeout("Not a single retry was fired")
