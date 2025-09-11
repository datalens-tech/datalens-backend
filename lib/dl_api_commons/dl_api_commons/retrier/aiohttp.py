import asyncio
import logging
from typing import (
    Any,
    Awaitable,
    Callable,
)

import aiohttp
import aiohttp.client_exceptions
import attr

from dl_api_commons.aiohttp.aiohttp_client import BaseRetrier
import dl_retrier


LOGGER = logging.getLogger(__name__)


class AiohttpRetryError(Exception):
    """Aiohttp retrier failed attempting to retry"""


class AiohttpRetryTimeout(aiohttp.client_exceptions.ServerTimeoutError, AiohttpRetryError):
    """Timed out attempting to retry"""


@attr.s(kw_only=True, frozen=True)
class AiohttpPolicyRetrier(BaseRetrier):
    """
    Retrier for Aiohttp client
    """

    _retry_policy: dl_retrier.RetryPolicy = attr.ib()

    # TODO: Merge with requests.RequestsPolicyRetrier as sync/async-independent implementation

    async def retry_request(
        self,
        req_func: Callable[..., Awaitable[aiohttp.ClientResponse]],
        method: str,
        *args: Any,
        **kwargs: Any,
    ) -> aiohttp.ClientResponse:
        last_known_result: Exception | aiohttp.ClientResponse | None = None

        for retry in self._retry_policy.iter_retries():
            if retry.sleep_before_seconds > 0:
                await asyncio.sleep(retry.sleep_before_seconds)

            kwargs["read_timeout_sec"] = retry.request_timeout
            kwargs["conn_timeout_sec"] = retry.connect_timeout

            try:
                resp = await req_func(method, *args, **kwargs)
            except aiohttp.client_exceptions.ClientError as err:
                LOGGER.warning("aiohttp client error", exc_info=True)
                last_known_result = err
                continue

            if not self._retry_policy.can_retry_error(resp.status):
                return resp

            last_known_result = resp

        if isinstance(last_known_result, aiohttp.ClientResponse):
            return last_known_result

        if isinstance(last_known_result, aiohttp.client_exceptions.ServerTimeoutError):
            raise AiohttpRetryTimeout() from last_known_result

        if isinstance(last_known_result, Exception):
            raise AiohttpRetryError() from last_known_result

        raise AiohttpRetryError("Not a single retry was fired")
