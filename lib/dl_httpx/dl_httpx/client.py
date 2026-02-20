import abc
import asyncio
import contextlib
import logging
import ssl
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

import dl_auth
import dl_configs
import dl_constants
from dl_httpx.models import BaseRequest
from dl_httpx.retry_mutator import RetryRequestMutator
import dl_retrier


LOGGER = logging.getLogger(__name__)

_REQUEST_HEADERS_TO_LOG = (dl_constants.DLHeadersCommon.REQUEST_ID.value,)


def _request_to_string(request: httpx.Request) -> str:
    headers = {header: request.headers[header] for header in _REQUEST_HEADERS_TO_LOG if header in request.headers}
    return f"Request(method={request.method}, url={request.url}, headers={headers})"


def _request_to_debug_string(request: httpx.Request) -> str:
    stream = request.read()
    return f"Request(method={request.method}, url={request.url}, headers={request.headers}, stream={stream!r})"


def _response_to_string(response: httpx.Response) -> str:
    return f"Response(status_code={response.status_code})"


def _response_to_debug_string(response: httpx.Response) -> str:
    content = response.content
    return f"Response(status_code={response.status_code}, headers={response.headers}, content={content!r})"


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
class HttpxClientDependencies:
    base_url: str
    base_cookies: dict[str, str] = attrs.field(factory=dict)
    base_headers: dict[str, str] = attrs.field(factory=dict)

    ssl_context: ssl.SSLContext = attrs.field(factory=dl_configs.get_default_ssl_context)
    retry_policy_factory: dl_retrier.BaseRetryPolicyFactory = attrs.field(factory=dl_retrier.DefaultRetryPolicyFactory)
    auth_provider: dl_auth.AuthProviderProtocol = attrs.field(factory=dl_auth.NoAuthProvider)
    logger: logging.Logger = attrs.field(default=LOGGER)
    debug_logging: bool = attrs.field(default=False)


@attrs.define(kw_only=True, auto_attribs=True, frozen=True)
class HttpxBaseClient(Generic[THttpxClient], abc.ABC):
    _base_url: str = attrs.field()
    _base_cookies: dict[str, str]
    _base_headers: dict[str, str]
    _retry_policy_factory: dl_retrier.BaseRetryPolicyFactory
    _auth_provider: dl_auth.AuthProviderProtocol
    _logger: logging.Logger
    _debug_logging: bool

    _base_client: THttpxClient

    @classmethod
    def from_dependencies(cls, dependencies: HttpxClientDependencies) -> typing_extensions.Self:
        return cls(
            base_url=dependencies.base_url,
            base_cookies=dependencies.base_cookies,
            base_headers=dependencies.base_headers,
            retry_policy_factory=dependencies.retry_policy_factory,
            auth_provider=dependencies.auth_provider,
            logger=dependencies.logger,
            debug_logging=dependencies.debug_logging,
            base_client=cls._get_client(
                ssl_context=dependencies.ssl_context,
            ),
        )

    @classmethod
    @abc.abstractmethod
    def _get_client(cls, ssl_context: ssl.SSLContext) -> THttpxClient:
        ...

    @property
    def _client_name(self) -> str:
        return self.__class__.__name__

    @property
    def _mutators(self) -> list[RetryRequestMutator]:
        return []

    def _prepare_url(self, url: str) -> str:
        return urljoin(self._base_url, url)

    def _prepare_headers(self, headers: dict[str, str] | None = None) -> dict[str, str]:
        result = self._base_headers.copy()
        result.update(self._auth_provider.get_headers())
        if headers:
            result.update(headers)
        return result

    def _prepare_cookies(self, cookies: dict[str, str] | None = None) -> dict[str, str]:
        result = self._base_cookies.copy()
        result.update(self._auth_provider.get_cookies())
        if cookies:
            result.update(cookies)
        return result

    def prepare_request(self, request: BaseRequest) -> httpx.Request:
        return self.prepare_raw_request(
            method=request.method,
            url=request.path,
            headers=request.headers,
            cookies=request.cookies,
            params=request.query_params,
            json=request.body,
        )

    def prepare_raw_request(
        self,
        method: str,
        url: str,
        headers: dict[str, str] | None = None,
        cookies: dict[str, str] | None = None,
        params: dict[str, str] | None = None,
        json: Any = None,
    ) -> httpx.Request:
        request = httpx.Request(
            method=method,
            url=self._prepare_url(url),
            headers=self._prepare_headers(headers),
            cookies=self._prepare_cookies(cookies),
            params=params,
            json=json,
        )

        if self._debug_logging:
            self._logger.debug(
                "%s prepared request: %s",
                self._client_name,
                _request_to_debug_string(request),
            )

        return request

    def _process_response(self, response: httpx.Response) -> httpx.Response:
        if self._debug_logging:
            self._logger.debug(
                "%s processing response: %s",
                self._client_name,
                _response_to_debug_string(response),
            )

        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as e:
            self._logger.error(
                "%s received HTTP status error: code=%s, text=%s",
                self._client_name,
                e.response.status_code,
                e.response.text,
            )
            raise HttpStatusHttpxClientException(
                request=e.request,
                response=e.response,
            ) from e

        return response


HttpxClientT = TypeVar("HttpxClientT", bound=HttpxBaseClient)


@attrs.define(kw_only=True, auto_attribs=True, frozen=True)
class HttpxSyncRetrier:
    _retry_policy: dl_retrier.RetryPolicy
    _client_name: str
    _logger: logging.Logger
    _mutators: list[RetryRequestMutator] = attrs.field(factory=list)

    def send(
        self,
        request_func: SyncRequestFunction,
        request: httpx.Request,
    ) -> httpx.Response:
        last_known_result: httpx.Response | Exception | None = None

        for retry in self._retry_policy.iter_retries():
            for mutator in self._mutators:
                mutator.on_retry(request, retry)

            if retry.sleep_before_seconds > 0:
                time.sleep(retry.sleep_before_seconds)

            timeout = httpx.Timeout(
                None,
                connect=retry.connect_timeout,
                read=retry.request_timeout,
            )
            request.extensions["timeout"] = timeout.as_dict()

            self._logger.debug(
                "%s sending Attempt(%s): %s",
                self._client_name,
                retry.attempt_number,
                _request_to_string(request),
            )

            try:
                response = request_func(request)
            except Exception as e:
                self._logger.exception(
                    "%s failed: %s",
                    self._client_name,
                    _request_to_string(request),
                )
                last_known_result = e
                continue

            self._logger.debug(
                "%s received %s for %s",
                self._client_name,
                _response_to_string(response),
                _request_to_string(request),
            )

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
    _retry_policy: dl_retrier.RetryPolicy
    _client_name: str
    _logger: logging.Logger
    _mutators: list[RetryRequestMutator] = attrs.field(factory=list)

    async def send(
        self,
        request_func: AsyncRequestFunction,
        request: httpx.Request,
    ) -> httpx.Response:
        last_known_result: httpx.Response | Exception | None = None

        for retry in self._retry_policy.iter_retries():
            for mutator in self._mutators:
                mutator.on_retry(request, retry)

            if retry.sleep_before_seconds > 0:
                await asyncio.sleep(retry.sleep_before_seconds)

            timeout = httpx.Timeout(
                None,
                connect=retry.connect_timeout,
                read=retry.request_timeout,
            )
            request.extensions["timeout"] = timeout.as_dict()

            self._logger.debug(
                "%s sending Attempt(%s): %s",
                self._client_name,
                retry.attempt_number,
                _request_to_string(request),
            )

            try:
                response = await request_func(request)
            except Exception as e:
                self._logger.exception(
                    "%s failed: %s",
                    self._client_name,
                    _request_to_string(request),
                )
                last_known_result = e
                continue

            self._logger.debug(
                "%s received %s for %s",
                self._client_name,
                _response_to_string(response),
                _request_to_string(request),
            )

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
    def _get_client(cls, ssl_context: ssl.SSLContext) -> httpx.Client:
        return httpx.Client(verify=ssl_context)

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
        retrier = HttpxSyncRetrier(
            retry_policy=retry_policy,
            logger=self._logger,
            client_name=self._client_name,
            mutators=self._mutators,
        )
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
    def _get_client(cls, ssl_context: ssl.SSLContext) -> httpx.AsyncClient:
        return httpx.AsyncClient(verify=ssl_context)

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
        retrier = HttpxAsyncRetrier(
            retry_policy=retry_policy,
            client_name=self._client_name,
            logger=self._logger,
            mutators=self._mutators,
        )
        response = await retrier.send(self._send, request)
        response = self._process_response(response)

        try:
            yield response
        finally:
            await response.aclose()

    async def _send(self, request: httpx.Request) -> httpx.Response:
        return await self._base_client.send(request=request)


__all__ = [
    "HttpStatusHttpxClientException",
    "HttpxAsyncClient",
    "HttpxBaseClient",
    "HttpxClientDependencies",
    "HttpxClientT",
    "HttpxSyncClient",
    "RequestHttpxClientException",
]
