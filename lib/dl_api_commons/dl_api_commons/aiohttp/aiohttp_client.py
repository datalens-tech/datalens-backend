from __future__ import annotations

from abc import (
    ABC,
    abstractmethod,
)
import asyncio
from contextlib import asynccontextmanager
import logging
import ssl
from types import TracebackType
from typing import (
    Any,
    AsyncGenerator,
    Awaitable,
    Callable,
    Optional,
    Type,
)

import aiohttp
import aiohttp.web
import attr


LOGGER = logging.getLogger(__name__)


THeaders = dict[str, str]
TCookies = dict[str, str]
TParams = dict[str, str]


class BaseRetrier(ABC):
    @abstractmethod
    async def retry_request(
        self, req_func: Callable[..., Awaitable[aiohttp.ClientResponse]], method: str, *args: Any, **kwargs: Any
    ) -> aiohttp.ClientResponse:
        """make some retry magic"""


class NoRetriesRetrier(BaseRetrier):
    async def retry_request(
        self, req_func: Callable[..., Awaitable[aiohttp.ClientResponse]], method: str, *args: Any, **kwargs: Any
    ) -> aiohttp.ClientResponse:
        return await req_func(method, *args, **kwargs)


@attr.s(kw_only=True, frozen=True)
class PredefinedIntervalsRetrier(BaseRetrier):
    retry_intervals: tuple[float, ...] = attr.ib(default=(0.5, 1.0, 2.0))
    retry_codes: set[int] = attr.ib(default={408, 429, 500, 502, 503, 504})
    retry_methods: set[str] = attr.ib(default={"GET"})
    retry_exception_classes: tuple[Type[Exception]] = attr.ib(default=(aiohttp.ClientError,))

    @retry_methods.validator
    def check_all_chars_upper(self, attribute: str, value: set[str]) -> None:
        for method in value:
            if not all("A" <= char <= "Z" for char in method):
                raise ValueError("method names should be in uppercase")

    def should_retry_exc(self, exc: Exception) -> bool:
        return any(isinstance(exc, retry_exc_cls) for retry_exc_cls in self.retry_exception_classes)

    async def retry_request(
        self, req_func: Callable[..., Awaitable[aiohttp.ClientResponse]], method: str, *args: Any, **kwargs: Any
    ) -> aiohttp.ClientResponse:
        if method.upper() not in self.retry_methods:
            return await req_func(method, *args, **kwargs)

        for idx, ret_interval in enumerate(self.retry_intervals):
            last = idx == len(self.retry_intervals) - 1

            try:
                resp = await req_func(method, *args, **kwargs)
            except Exception as err:
                if not self.should_retry_exc(err):
                    raise
                LOGGER.warning("aiohttp client error: %r", err)
                if last:
                    raise
            else:
                if resp.status in self.retry_codes:
                    LOGGER.warning("HTTP error: %r, ", resp.status)

                if resp.status not in self.retry_codes or last:
                    return resp

            await asyncio.sleep(ret_interval)

        raise Exception("You should not be here")


@attr.s(kw_only=True)
class BIAioHTTPClient:
    base_url: str = attr.ib()

    cookies: TCookies = attr.ib(factory=dict)
    headers: THeaders = attr.ib(factory=dict)

    conn_timeout_sec: float = attr.ib(default=0.7)
    read_timeout_sec: float = attr.ib(default=30)
    # TODO: total_timeout_sec (retries!)

    raise_for_status: bool = attr.ib(default=True)

    retrier: BaseRetrier = attr.ib(factory=NoRetriesRetrier)

    _ca_data: bytes = attr.ib()
    _session: Optional[aiohttp.ClientSession] = attr.ib(init=False)

    def __attrs_post_init__(self):  # type: ignore  # 2024-01-24 # TODO: Function is missing a return type annotation  [no-untyped-def]
        self._session = self._make_session()

    def _make_session(self) -> aiohttp.ClientSession:
        ssl_context = ssl.create_default_context(cadata=self._ca_data.decode("ascii"))
        return aiohttp.ClientSession(
            cookies=self.cookies,
            headers=self.headers,
            connector=aiohttp.TCPConnector(
                ssl_context=ssl_context,
            ),
        )

    async def close(self) -> None:
        await self._session.close()  # type: ignore  # 2024-01-24 # TODO: Item "None" of "ClientSession | None" has no attribute "close"  [union-attr]

    async def __aenter__(self) -> BIAioHTTPClient:
        return self

    async def __aexit__(
        self, exc_type: Optional[Type[BaseException]], exc_val: Optional[BaseException], exc_tb: Optional[TracebackType]
    ) -> None:
        await self.close()

    def url(self, path: str) -> str:
        return "/".join(map(lambda s: s.strip("/"), (self.base_url, path)))

    @asynccontextmanager
    async def request(self, method: str, *args: Any, **kwargs: Any) -> AsyncGenerator[aiohttp.ClientResponse, None]:
        response = await self.retrier.retry_request(self._request, method, *args, **kwargs)  # type: ignore  # TODO: fix
        if self.raise_for_status:
            response.raise_for_status()
        yield response
        response.close()

    async def _request(
        self,
        method: str,
        path: str = "",
        params: Optional[TParams] = None,
        data: Optional[Any] = None,
        json_data: Optional[Any] = None,
        headers: Optional[THeaders] = None,
        cookies: Optional[TCookies] = None,
        conn_timeout_sec: Optional[float] = None,
        read_timeout_sec: Optional[float] = None,
    ) -> Optional[Any]:
        timeout = aiohttp.ClientTimeout(
            sock_connect=conn_timeout_sec or self.conn_timeout_sec,
            sock_read=read_timeout_sec or self.read_timeout_sec,
        )
        return await self._session.request(  # type: ignore  # 2024-01-24 # TODO: Item "None" of "ClientSession | None" has no attribute "request"  [union-attr]
            method=method,
            url=self.url(path),
            params=params,
            data=data,
            json=json_data,
            headers=self.headers | (headers or {}),
            cookies=self.cookies | (cookies or {}),
            timeout=timeout,
        )
