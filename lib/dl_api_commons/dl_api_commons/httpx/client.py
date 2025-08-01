import abc
import contextlib
import logging
from types import TracebackType
from typing import (
    Any,
    AsyncGenerator,
    Generic,
    Iterator,
    Protocol,
    TypeVar,
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


THttpxClient = TypeVar("THttpxClient", httpx.Client, httpx.AsyncClient)


@attrs.define(kw_only=True)
class BIHttpxClientSettings:
    base_url: str = attrs.field()
    base_cookies: dict[str, str] = attrs.field(factory=dict)
    base_headers: dict[str, str] = attrs.field(factory=dict)


@attrs.define(kw_only=True)
class BIHttpxBaseClient(Generic[THttpxClient], abc.ABC):
    _settings: BIHttpxClientSettings

    _base_client: THttpxClient

    @classmethod
    def from_settings(cls, settings: BIHttpxClientSettings) -> Self:
        return cls(
            settings=settings,
            base_client=cls._get_client(settings),
        )

    @classmethod
    @abc.abstractmethod
    def _get_client(cls, settings: BIHttpxClientSettings) -> THttpxClient:
        ...

    def _prepare_url(self, url: str) -> str:
        return urljoin(self._settings.base_url, url)

    def _prepare_headers(self, headers: dict[str, str] | None = None) -> dict[str, str]:
        result = self._settings.base_headers.copy()
        if headers:
            result.update(headers)
        return result

    def _prepare_cookies(self, cookies: dict[str, str] | None = None) -> dict[str, str]:
        result = self._settings.base_cookies.copy()
        if cookies:
            result.update(cookies)
        return result

    def prepare_request(
        self,
        method: str,
        url: str,
        headers: dict[str, str] | None = None,
        cookies: dict[str, str] | None = None,
        params: dict[str, str] | None = None,
        json: Any = None,
    ) -> httpx.Request:
        return httpx.Request(
            method=method,
            url=self._prepare_url(url),
            headers=self._prepare_headers(headers),
            cookies=self._prepare_cookies(cookies),
            params=params,
            json=json,
        )


BIHttpxClientT = TypeVar("BIHttpxClientT", bound=BIHttpxBaseClient)


@attrs.define(kw_only=True)
class BIHttpxSyncClient(BIHttpxBaseClient[httpx.Client]):
    _base_client: httpx.Client

    @classmethod
    def _get_client(cls, settings: BIHttpxClientSettings) -> httpx.Client:
        return httpx.Client(
            base_url=settings.base_url,
            cookies=settings.base_cookies,
            headers=settings.base_headers,
        )

    def close(self) -> None:
        self._base_client.close()

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
        response = self._send(request)
        response.raise_for_status()
        yield response
        response.close()

    def _send(self, request: httpx.Request) -> httpx.Response:
        return self._base_client.send(request=request)


@attrs.define(kw_only=True)
class BIHttpxAsyncClient(BIHttpxBaseClient[httpx.AsyncClient]):
    _base_client: httpx.AsyncClient

    @classmethod
    def _get_client(cls, settings: BIHttpxClientSettings) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            base_url=settings.base_url,
            cookies=settings.base_cookies,
            headers=settings.base_headers,
        )

    async def close(self) -> None:
        await self._base_client.aclose()

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
        response = await self._send(request)
        response.raise_for_status()
        yield response
        await response.aclose()

    async def _send(self, request: httpx.Request) -> httpx.Response:
        return await self._base_client.send(request=request)


__all__ = [
    "BIHttpxBaseClient",
    "BIHttpxSyncClient",
    "BIHttpxAsyncClient",
    "BIHttpxClientSettings",
    "BIHttpxClientT",
]
