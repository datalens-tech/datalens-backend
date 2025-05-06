import abc
import contextlib
import logging
import ssl
from types import TracebackType
from typing import (
    Any,
    AsyncGenerator,
    Awaitable,
    Callable,
    Iterator,
)

import attrs
import httpx
from typing_extensions import Self


LOGGER = logging.getLogger(__name__)


class BaseRetrier(abc.ABC):
    @abc.abstractmethod
    def retry_request(
        self, req_func: Callable[..., httpx.Response], request: httpx.Request, *args: Any, **kwargs: Any
    ) -> httpx.Response:
        raise NotImplementedError()

    @abc.abstractmethod
    async def retry_request_async(
        self, req_func: Callable[..., Awaitable[httpx.Response]], request: httpx.Request, *args: Any, **kwargs: Any
    ) -> httpx.Response:
        raise NotImplementedError()


class NoRetriesRetrier(BaseRetrier):
    def retry_request(
        self, req_func: Callable[..., httpx.Response], request: httpx.Request, *args: Any, **kwargs: Any
    ) -> httpx.Response:
        return req_func(request, *args, **kwargs)

    async def retry_request_async(
        self, req_func: Callable[..., Awaitable[httpx.Response]], request: httpx.Request, *args: Any, **kwargs: Any
    ) -> httpx.Response:
        return await req_func(request, *args, **kwargs)


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
        return "/".join(map(lambda s: s.strip("/"), (self.base_url, path)))


@attrs.define(kw_only=True)
class BIHttpxClient(BIHttpxBaseClient):
    client: httpx.Client | None = attrs.field(init=False, default=None)

    def _make_client(self) -> httpx.Client:
        return httpx.Client(
            base_url=self.base_url,
            cookies=self.cookies,
            headers=self.headers,
            verify=self.verify,
            timeout=self.build_timeout(),
        )

    def close(self) -> None:
        if self.client is not None:
            self.client.close()
            self.client = None

    def __enter__(self) -> Self:
        if self.client is None:
            self.client = self._make_client()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_value: BaseException | None = None,
        traceback: TracebackType | None = None,
    ) -> None:
        self.close()

    @contextlib.contextmanager
    def send(self, request: httpx.Request, *args: Any, **kwargs: Any) -> Iterator[httpx.Response]:
        response = self.retrier.retry_request(self._send, request, *args, **kwargs)
        if self.raise_for_status:
            response.raise_for_status()
        yield response
        response.close()

    def _send(self, request: httpx.Request) -> httpx.Response:
        if self.client is None:
            self.client = self._make_client()

        return self.client.send(request=request)


@attrs.define(kw_only=True)
class BIHttpxAsyncClient(BIHttpxBaseClient):
    client: httpx.AsyncClient | None = attrs.field(init=False, default=None)

    def _make_client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            base_url=self.base_url,
            cookies=self.cookies,
            headers=self.headers,
            verify=self.verify,
            timeout=self.build_timeout(),
        )

    async def close(self) -> None:
        if self.client is not None:
            await self.client.aclose()
            self.client = None

    async def __aenter__(self) -> Self:
        if self.client is None:
            self.client = self._make_client()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_value: BaseException | None = None,
        traceback: TracebackType | None = None,
    ) -> None:
        await self.close()

    @contextlib.asynccontextmanager
    async def send(self, request: httpx.Request, *args: Any, **kwargs: Any) -> AsyncGenerator[httpx.Response, None]:
        response = await self.retrier.retry_request_async(self._send, request, *args, **kwargs)
        if self.raise_for_status:
            response.raise_for_status()
        yield response
        await response.aclose()

    async def _send(self, request: httpx.Request) -> httpx.Response:
        if self.client is None:
            self.client = self._make_client()

        return await self.client.send(request=request)


__all__ = [
    "BaseRetrier",
    "NoRetriesRetrier",
    "BIHttpxClient",
    "BIHttpxAsyncClient",
]
