# dl_httpx

Typed, retry-aware HTTP client wrapper around [httpx](https://www.python-httpx.org/). Provides sync and async clients with structured request/response models, pluggable authentication, automatic retry with configurable policies, and distributed request tracing via request IDs. This is the foundation layer for all `*_client` packages in the monorepo.

## Architecture

```
┌─────────────────────────────┐
│  Consumer Client Package    │  e.g. bi_snapter_client, bi_dls_client
│  (BaseXxxClient[HttpxClientT]) │
├─────────────────────────────┤
│  dl_httpx                   │  HttpxSyncClient / HttpxAsyncClient
│  BaseRequest, BaseSchema    │  retry, auth, tracing, error handling
├─────────────────────────────┤
│  httpx                      │  HTTP transport
└─────────────────────────────┘
```

Key internal dependencies: `dl_retrier` (retry policies), `dl_auth` (auth providers), `dl_pydantic` (schema base classes), `dl_constants` (header names), `dl_configs` (SSL defaults), `dl_json` (serialization), `dl_utils` (request ID generation).

## Installation

Add to your package's `pyproject.toml`:

```toml
[tool.poetry.dependencies]
dl-httpx = {path = "../../mainrepo/lib/dl_httpx"}  # adjust relative path
```

## Public API Reference

All exports are available from `dl_httpx` directly.

### Clients

| Class | Description |
|---|---|
| `HttpxBaseClient[THttpxClient]` | Abstract generic base; parametrized by `httpx.Client` or `httpx.AsyncClient` |
| `HttpxSyncClient` | Synchronous client. Context manager (`with`). `send()` returns `Iterator[httpx.Response]` |
| `HttpxAsyncClient` | Asynchronous client. Async context manager (`async with`). `send()` returns `AsyncGenerator[httpx.Response]` |
| `HttpxClientT` | TypeVar bound to `HttpxBaseClient`, used for generic consumer clients |

### Configuration

| Class | Description |
|---|---|
| `HttpxClientDependencies` | Frozen attrs container with all client config (see [Configuration](#configuration) section) |

### Request Models

| Class | Description |
|---|---|
| `BaseRequest` | Abstract attrs class. Subclass and implement `path` and `method` properties |
| `BaseSchema` | Re-export of `dl_pydantic.BaseSchema`. Use for request body data models |
| `BaseResponseSchema` | Extends `BaseSchema`. Use for response data models |
| `TypedBaseSchema` | For discriminated unions (polymorphic models). Call `.register(key, subclass)` |
| `TypedSchemaAnnotation` | Pydantic annotation type for `TypedBaseSchema` fields |
| `TypedSchemaListAnnotation` | Annotation for lists of typed schemas |
| `TypedSchemaDictAnnotation` | Annotation for dicts of typed schemas |
| `ParentContext` | Carries `request_id` from parent for hierarchical tracing |
| `ParentContextProtocol` | Protocol for anything that provides a `request_id` property |

### Exceptions

| Class | Attributes | When raised |
|---|---|---|
| `HttpStatusHttpxClientException` | `request`, `response` | HTTP 4xx/5xx after retries |
| `RequestHttpxClientException` | `original_exception` | Network error after all retries exhausted |
| `NoRetriesHttpxClientException` | — | Retry policy yields zero retries (edge case) |

### Retry

| Class | Description |
|---|---|
| `RetryRequestMutator` | Protocol: `on_retry(request, retry) -> None`. Hook called on each retry attempt |
| `RequestIdRetryMutator` | Appends `/N` to request ID on retries (e.g. `abc123` → `abc123/2` → `abc123/3`) |

### Testing

| Class | Description |
|---|---|
| `TestingHttpxClient` | Wraps sync + async clients. Any method call invokes both and asserts equal results |

## Usage Patterns

### Building a New Client Package

All consumer packages follow the same canonical structure. Below is a complete, minimal example based on real monorepo patterns (e.g. `bi_snapter_client`).

#### 1. Dependencies & Settings

```python
# my_client/clients/base.py
import abc
from typing import Generic

import attrs
import httpx
import pydantic
from typing_extensions import Self

import dl_auth
import dl_httpx
import dl_retrier
import dl_settings


@attrs.define(kw_only=True, frozen=True)
class MyClientDependencies(dl_httpx.HttpxClientDependencies):
    pass  # extend with custom fields if needed


class MyRetryPolicySettings(dl_retrier.RetryPolicySettings):
    connect_timeout: float = 5
    request_timeout: float = 30
    retries_count: int = 3


class MyRetryPolicyFactorySettings(dl_retrier.RetryPolicyFactorySettings):
    DEFAULT_POLICY: MyRetryPolicySettings = pydantic.Field(
        default_factory=MyRetryPolicySettings,
    )


class MyClientSettings(dl_settings.BaseSettings):
    BASE_URL: str
    RETRY_POLICY_FACTORY: MyRetryPolicyFactorySettings = pydantic.Field(
        default_factory=MyRetryPolicyFactorySettings,
    )
    AUTH_PROVIDER: dl_settings.TypedAnnotation[dl_auth.AuthProviderSettings]
```

#### 2. Generic Base Client

```python
# my_client/clients/base.py (continued)

@attrs.define(kw_only=True)
class BaseMyClient(Generic[dl_httpx.HttpxClientT], abc.ABC):
    _base_client: dl_httpx.HttpxClientT

    @classmethod
    def from_dependencies(cls, dependencies: MyClientDependencies) -> Self:
        return cls(base_client=cls._get_base_client(dependencies))

    @classmethod
    @abc.abstractmethod
    def _get_base_client(cls, dependencies: MyClientDependencies) -> dl_httpx.HttpxClientT:
        ...

    def _prepare_request(self, request: dl_httpx.BaseRequest) -> httpx.Request:
        return self._base_client.prepare_request(request=request)

    # Define response processing methods here (shared by sync and async)
    def _get_items_process_response(self, response: httpx.Response) -> list[ItemResponse]:
        raw_data = response.json()
        return [ItemResponse.model_validate(item) for item in raw_data["items"]]
```

#### 3. Async Client

```python
# my_client/clients/client_async.py
import logging

import attrs
import httpx

import dl_httpx
from my_client.clients.base import BaseMyClient, MyClientDependencies
from my_client.clients.exceptions import handle_http_status_error

LOGGER = logging.getLogger(__name__)


@attrs.define(kw_only=True)
class MyAsyncClient(BaseMyClient[dl_httpx.HttpxAsyncClient]):
    @classmethod
    def _get_base_client(
        cls,
        dependencies: MyClientDependencies,
    ) -> dl_httpx.HttpxAsyncClient:
        return dl_httpx.HttpxAsyncClient.from_dependencies(
            dependencies=dl_httpx.HttpxClientDependencies(
                base_url=dependencies.base_url,
                ssl_context=dependencies.ssl_context,
                retry_policy_factory=dependencies.retry_policy_factory,
                auth_provider=dependencies.auth_provider,
                logger=LOGGER,
                debug_logging=dependencies.debug_logging,
            ),
        )

    async def close(self) -> None:
        await self._base_client.close()

    async def _send(self, request: httpx.Request) -> httpx.Response:
        try:
            async with self._base_client.send(request=request) as response:
                return response
        except dl_httpx.HttpStatusHttpxClientException as e:
            raise handle_http_status_error(e) from e

    async def get_items(self, request: GetItemsRequest) -> list[ItemResponse]:
        response = await self._send(self._prepare_request(request))
        return self._get_items_process_response(response)
```

#### 4. Sync Client

```python
# my_client/clients/client_sync.py
import logging

import attrs
import httpx

import dl_httpx
from my_client.clients.base import BaseMyClient, MyClientDependencies
from my_client.clients.exceptions import handle_http_status_error

LOGGER = logging.getLogger(__name__)


@attrs.define(kw_only=True)
class MySyncClient(BaseMyClient[dl_httpx.HttpxSyncClient]):
    @classmethod
    def _get_base_client(
        cls,
        dependencies: MyClientDependencies,
    ) -> dl_httpx.HttpxSyncClient:
        return dl_httpx.HttpxSyncClient.from_dependencies(
            dependencies=dl_httpx.HttpxClientDependencies(
                base_url=dependencies.base_url,
                ssl_context=dependencies.ssl_context,
                retry_policy_factory=dependencies.retry_policy_factory,
                auth_provider=dependencies.auth_provider,
                logger=LOGGER,
                debug_logging=dependencies.debug_logging,
            ),
        )

    def close(self) -> None:
        self._base_client.close()

    def _send(self, request: httpx.Request) -> httpx.Response:
        try:
            with self._base_client.send(request=request) as response:
                return response
        except dl_httpx.HttpStatusHttpxClientException as e:
            raise handle_http_status_error(e) from e

    def get_items(self, request: GetItemsRequest) -> list[ItemResponse]:
        response = self._send(self._prepare_request(request))
        return self._get_items_process_response(response)
```

#### 5. Request & Response Models

```python
# my_client/models/request.py
import attrs

import dl_constants
import dl_httpx
import dl_json


@attrs.define(kw_only=True, frozen=True)
class GetItemsRequest(dl_httpx.BaseRequest):
    tenant_id: str
    page: int = 1

    @property
    def path(self) -> str:
        return "/api/v1/items"

    @property
    def method(self) -> str:
        return "GET"

    @property
    def query_params(self) -> dict[str, str]:
        return {"page": str(self.page)}

    @property
    def headers(self) -> dict[str, str]:
        return {
            **super().headers,  # includes auto-generated Request-ID
            dl_constants.DLHeadersCommon.TENANT_ID.value: self.tenant_id,
        }


@attrs.define(kw_only=True, frozen=True)
class CreateItemRequest(dl_httpx.BaseRequest):
    data: CreateItemData

    @property
    def path(self) -> str:
        return "/api/v1/items"

    @property
    def method(self) -> str:
        return "POST"

    @property
    def body(self) -> dl_json.JsonSerializableMapping:
        return self.data.model_get_body(exclude_none=True)
```

```python
# my_client/models/data.py
import pydantic

import dl_httpx


class CreateItemData(dl_httpx.BaseSchema):
    name: str
    description: str | None = None
    tags: list[str] = pydantic.Field(default_factory=list)
```

```python
# my_client/models/response.py
import dl_httpx


class ItemResponse(dl_httpx.BaseResponseSchema):
    id: str
    name: str
    status: str
```

#### 6. Error Handling

```python
# my_client/clients/exceptions.py
import attrs

import dl_httpx


@attrs.define(kw_only=True)
class MyClientException(Exception):
    code: str | None = None
    message: str | None = None


class NotFoundError(MyClientException):
    pass


class AccessDeniedError(MyClientException):
    pass


_CODE_TO_EXCEPTION: dict[str, type[MyClientException]] = {
    "NOT_FOUND": NotFoundError,
    "ACCESS_DENIED": AccessDeniedError,
}


def handle_http_status_error(
    exception: dl_httpx.HttpStatusHttpxClientException,
) -> MyClientException:
    response = exception.response

    code: str | None = None
    message: str | None = None

    try:
        body = response.json()
        if isinstance(body, dict):
            code = body.get("code")
            message = body.get("message")
    except Exception:
        message = response.text or "Unknown error"

    exception_class = (
        _CODE_TO_EXCEPTION.get(code, MyClientException) if code else MyClientException
    )
    return exception_class(code=code, message=message)
```

## Configuration

`HttpxClientDependencies` fields:

| Field | Type | Default | Description |
|---|---|---|---|
| `base_url` | `str` | *required* | Base URL for all requests (joined with request `path` via `urljoin`) |
| `base_cookies` | `dict[str, str]` | `{}` | Cookies added to every request |
| `base_headers` | `dict[str, str]` | `{}` | Headers added to every request |
| `ssl_context` | `ssl.SSLContext` | `dl_configs.get_default_ssl_context()` | SSL/TLS verification context |
| `retry_policy_factory` | `dl_retrier.BaseRetryPolicyFactory` | `DefaultRetryPolicyFactory` | Factory that produces retry policies by name |
| `auth_provider` | `dl_auth.AuthProviderProtocol` | `NoAuthProvider` | Provides auth headers/cookies for all requests |
| `logger` | `logging.Logger` | module logger | Logger for request/response tracing |
| `debug_logging` | `bool` | `False` | When `True`, logs full request/response bodies |

Header/cookie merge order (later overrides earlier):
1. `base_headers` / `base_cookies` (from dependencies)
2. Auth provider headers/cookies
3. Per-request headers/cookies (from `BaseRequest` subclass)

## Error Handling

Catch `HttpStatusHttpxClientException` in your `_send` method and translate to domain-specific exceptions:

```python
async def _send(self, request: httpx.Request) -> httpx.Response:
    try:
        async with self._base_client.send(request=request) as response:
            return response
    except dl_httpx.HttpStatusHttpxClientException as e:
        raise handle_http_status_error(e) from e
```

The exception carries both `request` and `response`, so you can inspect status codes and parse error bodies.

`RequestHttpxClientException` wraps network-level errors (DNS, connection refused, timeout after all retries). Its `original_exception` attribute holds the underlying error.

## Request Tracing

- Every `BaseRequest` auto-generates a UUID-based `request_id`
- Pass `parent_context=ParentContext(request_id=parent_id)` to create hierarchical IDs: `{parent}--{child}`
- The request ID is automatically included in the `Request-ID` header
- Use `RequestIdRetryMutator` to append `/N` suffixes on retries for tracing: `abc123` → `abc123/2` → `abc123/3`

## Testing

Use `TestingHttpxClient` to verify sync/async parity. It wraps both clients and asserts that any method call produces identical results from both:

```python
import dl_httpx

testing_client = dl_httpx.TestingHttpxClient(
    sync_client=sync_client,
    async_client=async_client,
)

# Calls both sync and async implementations, asserts results are equal
result = await testing_client.get_items(request)
```

Use [respx](https://github.com/lundberg/respx) for HTTP mocking in tests (httpx-compatible).

## Gotchas & Tips

- **Always create the generic base + async, even if you only need async.** The pattern is `BaseMyClient[HttpxClientT]` + `MyAsyncClient(BaseMyClient[HttpxAsyncClient])`. This makes adding sync trivial later and keeps the codebase consistent with other clients.
- **`send()` returns a context manager, not a response.** Use `with client.send(request) as response:` (sync) or `async with client.send(request) as response:` (async). The response is closed when the context exits.
- **All attrs classes are frozen (immutable).** `HttpxClientDependencies`, `BaseRequest`, exceptions — all use `frozen=True`.
- **`super().headers` in request subclasses.** Always call `**super().headers` when overriding `headers` to preserve the auto-generated `Request-ID` header.
- **URL joining uses `urljoin`.** The request `path` is joined with `base_url` via Python's `urllib.parse.urljoin`. Use relative paths (e.g. `/api/v1/items`) in your request's `path` property.

## Related Packages

| Package | Relationship |
|---|---|
| `dl_retrier` | Provides retry policy framework (`RetryPolicySettings`, `RetryPolicyFactory`) |
| `dl_auth` | Provides `AuthProviderProtocol` and concrete auth providers |
| `dl_pydantic` | Base schema classes re-exported as `dl_httpx.BaseSchema`, `TypedBaseSchema` |
| `dl_constants` | Header name constants (`DLHeadersCommon.REQUEST_ID`, etc.) |
| `dl_settings` | Settings base class used in client settings patterns |
| `dl_json` | `JsonSerializableMapping` type used by `BaseRequest.body` |

### Consumer Packages

`bi_snapter_client`, `bi_dls_client`, `bi_subscription_client`, `bi_staff_client`, `bi_client_notify`, `bi_vector_storage_api_client`, `bi_vector_storage_api_lib`, `bi_subscription_worker_lib`, `dl_zitadel`
