import abc
import asyncio
import contextlib
import logging
import time
import types
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
import typing_extensions

from dl_api_commons.retrier import (
    RetryPolicy,
    RetryPolicyFactory,
    RetryPolicyFactorySettings,
)


LOGGER = logging.getLogger(__name__)


class SyncRequestFunction(Protocol):
    def __call__(self, request: httpx.Request) -> httpx.Response:
        ...


class AsyncRequestFunction(Protocol):
    async def __call__(self, request: httpx.Request) -> httpx.Response:
        ...


THttpxClient = TypeVar("THttpxClient", httpx.Client, httpx.AsyncClient)


class BaseHttpxClientException(Exception):
    ...


@attrs.define(kw_only=True, auto_attribs=True, frozen=True)
class HttpStatusHttpxClientException(BaseHttpxClientException):
    request: httpx.Request
    response: httpx.Response


@attrs.define(kw_only=True, auto_attribs=True, frozen=True)
class RequestHttpxClientException(BaseHttpxClientException):
    original_exception: Exception


@attrs.define(kw_only=True, auto_attribs=True, frozen=True)
class NoRetriesHttpxClientException(BaseHttpxClientException):
    ...


@attrs.define(kw_only=True, auto_attribs=True, frozen=True)
class HttpxClientSettings:
    base_url: str
    base_cookies: dict[str, str] = attrs.field(factory=dict)
    base_headers: dict[str, str] = attrs.field(factory=dict)
    retry_policy_factory: RetryPolicyFactorySettings = attrs.field(factory=RetryPolicyFactorySettings)


@attrs.define(kw_only=True, auto_attribs=True, frozen=True)
class HttpxBaseClient(Generic[THttpxClient], abc.ABC):
    _base_url: str = attrs.field()
    _base_cookies: dict[str, str]
    _base_headers: dict[str, str]
    _retry_policy_factory: RetryPolicyFactory

    _base_client: THttpxClient

    @classmethod
    def from_settings(cls, settings: HttpxClientSettings) -> typing_extensions.Self:
        return cls(
            base_url=settings.base_url,
            base_cookies=settings.base_cookies,
            base_headers=settings.base_headers,
            retry_policy_factory=RetryPolicyFactory.from_settings(settings.retry_policy_factory),
            base_client=cls._get_client(),
        )

    @classmethod
    @abc.abstractmethod
    def _get_client(cls) -> THttpxClient:
        ...

    def _prepare_url(self, url: str) -> str:
        return urljoin(self._base_url, url)

    def _prepare_headers(self, headers: dict[str, str] | None = None) -> dict[str, str]:
        result = self._base_headers.copy()
        if headers:
            result.update(headers)
        return result

    def _prepare_cookies(self, cookies: dict[str, str] | None = None) -> dict[str, str]:
        result = self._base_cookies.copy()
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

    def _process_response(self, response: httpx.Response) -> httpx.Response:
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as e:
            raise HttpStatusHttpxClientException(
                request=e.request,
                response=e.response,
            ) from e

        return response


HttpxClientT = TypeVar("HttpxClientT", bound=HttpxBaseClient)


@attrs.define(kw_only=True, auto_attribs=True, frozen=True)
class HttpxSyncRetrier:
    _retry_policy: RetryPolicy

    def send(self, request_func: SyncRequestFunction, request: httpx.Request) -> httpx.Response:
        last_known_result: httpx.Response | Exception | None = None

        for retry in self._retry_policy.iter_retries():
            if retry.sleep_before_seconds > 0:
                time.sleep(retry.sleep_before_seconds)

            request.extensions["timeout"] = httpx.Timeout(
                None,
                connect=retry.connect_timeout,
                read=retry.request_timeout,
            )

            try:
                response = request_func(request)
            except Exception as e:
                LOGGER.warning("httpx client error", exc_info=True)
                last_known_result = e
                continue

            if not self._retry_policy.can_retry_error(response.status_code):
                return response

            last_known_result = response

        if isinstance(last_known_result, httpx.Response):
            return last_known_result

        if isinstance(last_known_result, Exception):
            raise RequestHttpxClientException(original_exception=last_known_result) from last_known_result

        raise NoRetriesHttpxClientException()


@attrs.define(kw_only=True, auto_attribs=True, frozen=True)
class HttpxAsyncRetrier:
    _retry_policy: RetryPolicy

    async def send(self, request_func: AsyncRequestFunction, request: httpx.Request) -> httpx.Response:
        last_known_result: httpx.Response | Exception | None = None

        for retry in self._retry_policy.iter_retries():
            if retry.sleep_before_seconds > 0:
                await asyncio.sleep(retry.sleep_before_seconds)

            request.extensions["timeout"] = httpx.Timeout(
                None,
                connect=retry.connect_timeout,
                read=retry.request_timeout,
            )

            try:
                response = await request_func(request)
            except Exception as e:
                LOGGER.warning("httpx client error", exc_info=True)
                last_known_result = e
                continue

            if not self._retry_policy.can_retry_error(response.status_code):
                return response

            last_known_result = response

        if isinstance(last_known_result, httpx.Response):
            return last_known_result

        if isinstance(last_known_result, Exception):
            raise RequestHttpxClientException(original_exception=last_known_result) from last_known_result

        raise NoRetriesHttpxClientException()


@attrs.define(kw_only=True, auto_attribs=True, frozen=True)
class HttpxSyncClient(HttpxBaseClient[httpx.Client]):
    _base_client: httpx.Client

    @classmethod
    def _get_client(cls) -> httpx.Client:
        return httpx.Client()

    def close(self) -> None:
        self._base_client.close()

    def __enter__(self) -> typing_extensions.Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_value: BaseException | None = None,
        traceback: types.TracebackType | None = None,
    ) -> None:
        self.close()

    @contextlib.contextmanager
    def send(
        self,
        request: httpx.Request,
        retry_policy_name: str | None = None,
    ) -> Iterator[httpx.Response]:
        retry_policy = self._retry_policy_factory.get_policy(retry_policy_name)
        retrier = HttpxSyncRetrier(retry_policy=retry_policy)
        response = retrier.send(self._send, request)
        response = self._process_response(response)

        try:
            yield response
        finally:
            response.close()

    def _send(
        self,
        request: httpx.Request,
    ) -> httpx.Response:
        return self._base_client.send(request=request)


@attrs.define(kw_only=True, auto_attribs=True, frozen=True)
class HttpxAsyncClient(HttpxBaseClient[httpx.AsyncClient]):
    _base_client: httpx.AsyncClient

    @classmethod
    def _get_client(cls) -> httpx.AsyncClient:
        return httpx.AsyncClient()

    async def close(self) -> None:
        await self._base_client.aclose()

    async def __aenter__(self) -> typing_extensions.Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_value: BaseException | None = None,
        traceback: types.TracebackType | None = None,
    ) -> None:
        await self.close()

    @contextlib.asynccontextmanager
    async def send(
        self,
        request: httpx.Request,
        retry_policy_name: str | None = None,
    ) -> AsyncGenerator[httpx.Response, None]:
        retry_policy = self._retry_policy_factory.get_policy(retry_policy_name)
        retrier = HttpxAsyncRetrier(retry_policy=retry_policy)
        response = await retrier.send(self._send, request)
        response = self._process_response(response)

        try:
            yield response
        finally:
            await response.aclose()

    async def _send(self, request: httpx.Request) -> httpx.Response:
        return await self._base_client.send(request=request)


__all__ = [
    "HttpxBaseClient",
    "HttpxSyncClient",
    "HttpxAsyncClient",
    "HttpxClientSettings",
    "HttpxClientT",
    "HttpStatusHttpxClientException",
    "RequestHttpxClientException",
]
