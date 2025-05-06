import abc
import contextlib
import logging
import ssl
from types import TracebackType
from typing import (
    AsyncGenerator,
    Iterator,
    Protocol,
)
from urllib.parse import urljoin

import attrs
import httpx
from typing_extensions import Self


LOGGER = logging.getLogger(__name__)


class SyncHandler(Protocol):
    def __call__(self, request: httpx.Request) -> httpx.Response:
        ...


class AsyncHandler(Protocol):
    async def __call__(self, request: httpx.Request) -> httpx.Response:
        ...


class BaseRetrier(abc.ABC):
    @abc.abstractmethod
    def retry_request(self, req_func: SyncHandler, request: httpx.Request) -> httpx.Response:
        raise NotImplementedError()

    @abc.abstractmethod
    async def retry_request_async(self, req_func: AsyncHandler, request: httpx.Request) -> httpx.Response:
        raise NotImplementedError()


class NoRetriesRetrier(BaseRetrier):
    def retry_request(self, req_func: SyncHandler, request: httpx.Request) -> httpx.Response:
        return req_func(request)

    async def retry_request_async(self, req_func: AsyncHandler, request: httpx.Request) -> httpx.Response:
        return await req_func(request)


@attrs.define(kw_only=True)
class BIHttpxBaseClient:
    base_url: str = attrs.field()

    cookies: dict[str, str] = attrs.field(factory=dict)
    headers: dict[str, str] = attrs.field(factory=dict)

    conn_timeout_sec: float = attrs.field(default=1.0)
    read_timeout_sec: float = attrs.field(default=10.0)
    write_timeout_sec: float = attrs.field(default=30.0)

    verify: str | bool | ssl.SSLContext = attrs.field(default=True)

    raise_for_status: bool = attrs.field(default=True)

    retrier: BaseRetrier = attrs.field(factory=NoRetriesRetrier)

    def build_timeout(self) -> httpx.Timeout:
        return httpx.Timeout(
            connect=self.conn_timeout_sec,
            read=self.read_timeout_sec,
            write=self.write_timeout_sec,
            pool=None,
        )

    def url(self, path: str) -> str:
        return urljoin(self.base_url, path)


@attrs.define(kw_only=True)
class BIHttpxClient(BIHttpxBaseClient):
    client: httpx.Client = attrs.field(init=False)

    def __attrs_post_init__(self) -> None:
        self.client = httpx.Client(
            base_url=self.base_url,
            cookies=self.cookies,
            headers=self.headers,
            verify=self.verify,
            timeout=self.build_timeout(),
        )

    def close(self) -> None:
        self.client.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_value: BaseException | None = None,
        traceback: TracebackType | None = None,
    ) -> None:
        self.close()

    @contextlib.contextmanager
    def send(self, request: httpx.Request) -> Iterator[httpx.Response]:
        response = self.retrier.retry_request(self._send, request)
        if self.raise_for_status:
            response.raise_for_status()
        yield response
        response.close()

    def _send(self, request: httpx.Request) -> httpx.Response:
        return self.client.send(request=request)


@attrs.define(kw_only=True)
class BIHttpxAsyncClient(BIHttpxBaseClient):
    client: httpx.AsyncClient = attrs.field(init=False)

    def __attrs_post_init__(self) -> None:
        self.client = httpx.AsyncClient(
            base_url=self.base_url,
            cookies=self.cookies,
            headers=self.headers,
            verify=self.verify,
            timeout=self.build_timeout(),
        )

    async def close(self) -> None:
        await self.client.aclose()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_value: BaseException | None = None,
        traceback: TracebackType | None = None,
    ) -> None:
        await self.close()

    @contextlib.asynccontextmanager
    async def send(self, request: httpx.Request) -> AsyncGenerator[httpx.Response, None]:
        response = await self.retrier.retry_request_async(self._send, request)
        if self.raise_for_status:
            response.raise_for_status()
        yield response
        await response.aclose()

    async def _send(self, request: httpx.Request) -> httpx.Response:
        return await self.client.send(request=request)


__all__ = [
    "BaseRetrier",
    "NoRetriesRetrier",
    "BIHttpxClient",
    "BIHttpxAsyncClient",
]
