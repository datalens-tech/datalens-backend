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
| `BaseResponseSchema` | Extends `BaseSchema`. Use for response data models. On `pydantic.ValidationError` from `model_validate` / `model_validate_json`, logs the offending raw payload and re-raises (see [Response validation logging](#response-validation-logging)) |
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

### Error transformers

| Symbol | Description |
|---|---|
| `ErrorTransformerProtocol` | Protocol: `transform(exception: HttpStatusHttpxClientException) -> Exception \| None`. Returns a domain exception or `None` to pass through |
| `ExceptionFactoryProtocol` | Callable Protocol: `__call__(exception: HttpStatusHttpxClientException) -> Exception`. Used as map values; satisfied by any callable, including bound classmethods like `MyError.from_httpx_exception` |
| `NullErrorTransformer` | No-op transformer; always returns `None` |
| `NULL_ERROR_TRANSFORMER` | Module-level singleton instance of `NullErrorTransformer`. Default for both class- and method-level |
| `CodeMapTransformer` | Maps a code value (extracted from the response body via `status_body_path: tuple[str, ...] = ("code",)`) to a factory; calls the factory on hit |
| `StatusMapTransformer` | Maps HTTP status code to a factory; calls the factory on hit |
| `ChainTransformer` | Tries a list of transformers in order; first non-`None` wins |

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
from my_client.clients.exceptions import MY_ERROR_TRANSFORMER

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
                error_transformer=MY_ERROR_TRANSFORMER,
                logger=LOGGER,
                debug_logging=dependencies.debug_logging,
            ),
        )

    async def close(self) -> None:
        await self._base_client.close()

    async def _send(self, request: httpx.Request) -> httpx.Response:
        async with self._base_client.send(request=request) as response:
            return response

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
from my_client.clients.exceptions import MY_ERROR_TRANSFORMER

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
                error_transformer=MY_ERROR_TRANSFORMER,
                logger=LOGGER,
                debug_logging=dependencies.debug_logging,
            ),
        )

    def close(self) -> None:
        self._base_client.close()

    def _send(self, request: httpx.Request) -> httpx.Response:
        with self._base_client.send(request=request) as response:
            return response

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

> **Always inherit response models from `BaseResponseSchema`** (directly or by mixing it in alongside an externally-defined schema, e.g. `class GetItemResponse(SharedItemSchema, dl_httpx.BaseResponseSchema): ...`). On a `pydantic.ValidationError` raised from `model_validate` / `model_validate_json`, `BaseResponseSchema` logs the offending raw payload at `ERROR` (via the `dl_httpx.models.base` logger) before re-raising. Plain `dl_pydantic.BaseSchema` will not log on failure, leaving you with a `ValidationError` that says *which* field is broken but not *what* the server actually returned.

#### 6. Error Handling

```python
# my_client/clients/exceptions.py
import attrs
from typing_extensions import Self

import dl_httpx


@attrs.define(kw_only=True)
class MyClientException(Exception):
    code: str | None = None
    message: str | None = None

    @classmethod
    def from_httpx_exception(cls, exception: dl_httpx.HttpStatusHttpxClientException) -> Self:
        body = exception.response.json()
        return cls(code=body.get("code"), message=body.get("message"))


class NotFoundError(MyClientException):
    pass


class AccessDeniedError(MyClientException):
    pass


MY_ERROR_TRANSFORMER: dl_httpx.ErrorTransformerProtocol = dl_httpx.CodeMapTransformer(
    code_map={
        "NOT_FOUND": NotFoundError.from_httpx_exception,
        "ACCESS_DENIED": AccessDeniedError.from_httpx_exception,
    },
)
```

Pass `MY_ERROR_TRANSFORMER` via `HttpxClientDependencies.error_transformer` (see the async/sync client examples above) so every endpoint of the client gets it. For per-endpoint specialization, attach an `error_transformer` field to the specific request model and forward `request.error_transformer` to `send()` from that endpoint method (see the [Per-call override](#per-call-override-carried-on-the-request) section below).

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

Translate `HttpStatusHttpxClientException` into domain-specific exceptions declaratively. Configure the transformer at the **class level** by passing it via `HttpxClientDependencies.error_transformer` (applies to every endpoint of the client), and optionally override it **per call** via the `error_transformer=` keyword on `send()` — typically by carrying it as a field on the request model.

### Defining domain exceptions

Domain exception classes are plain `attrs` classes. To use one as a factory in `CodeMapTransformer` / `StatusMapTransformer`, expose a classmethod (conventionally named `from_httpx_exception`) that takes the framework exception and returns an instance:

```python
import attrs
from typing_extensions import Self

import dl_httpx


@attrs.define(kw_only=True)
class MyClientException(Exception):
    code: str | None = None
    message: str | None = None

    @classmethod
    def from_httpx_exception(cls, exception: dl_httpx.HttpStatusHttpxClientException) -> Self:
        body = exception.response.json()
        return cls(code=body.get("code"), message=body.get("message"))


class NotFoundError(MyClientException):
    pass


class AccessDeniedError(MyClientException):
    pass
```

The bound classmethod (`MyClientException.from_httpx_exception`) is a callable that satisfies `ExceptionFactoryProtocol` — that's what the framework's transformers expect as map values. Lambdas and plain functions work too.

### Built-in transformers

- `CodeMapTransformer({"NOT_FOUND": NotFoundError.from_httpx_exception, ...}, status_body_path=("code",))` — extracts a code value from the response body by walking `status_body_path` (default `("code",)` reads `body["code"]`; `("error", "code")` reads `body["error"]["code"]`) and calls the mapped factory. Returns `None` if the body isn't JSON, doesn't have a value at the path, or the value isn't in the map.
- `StatusMapTransformer({404: NotFoundError.from_httpx_exception, 403: AccessDeniedError.from_httpx_exception})` — looks up the HTTP status code; calls the mapped factory on hit. Returns `None` on miss.
- `ChainTransformer([first, second, ...])` — runs each child in order; first non-`None` wins. Useful for combining a code map with a status fallback, or chaining two `CodeMapTransformer`s with different `status_body_path` settings.

Transformers are stateless and `frozen=True`; share instances freely.

### Class-level configuration

Pass the transformer via `HttpxClientDependencies.error_transformer`:

```python
MY_ERROR_TRANSFORMER = dl_httpx.ChainTransformer([
    dl_httpx.CodeMapTransformer({
        "NOT_FOUND": NotFoundError.from_httpx_exception,
        "ACCESS_DENIED": AccessDeniedError.from_httpx_exception,
    }),
    dl_httpx.StatusMapTransformer({404: NotFoundError.from_httpx_exception}),
])


class MyAsyncClient(BaseMyClient[dl_httpx.HttpxAsyncClient]):
    @classmethod
    def _get_base_client(cls, dependencies):
        return dl_httpx.HttpxAsyncClient.from_dependencies(
            dependencies=dl_httpx.HttpxClientDependencies(
                base_url=dependencies.base_url,
                ssl_context=dependencies.ssl_context,
                error_transformer=MY_ERROR_TRANSFORMER,
                ...
            ),
        )
```

Now every call through `send()` runs the transformer when a 4xx/5xx is received. If it returns `None`, the original `HttpStatusHttpxClientException` is raised unchanged.

### Per-call override (carried on the request)

When only one endpoint needs special-cased error handling, attach the transformer as a field on its request model:

```python
@attrs.define(kw_only=True, frozen=True)
class GetItemRequest(dl_httpx.BaseRequest):
    item_id: str
    error_transformer: dl_httpx.ErrorTransformerProtocol = dl_httpx.StatusMapTransformer(
        status_map={404: ItemNotFoundError.from_httpx_exception},
    )

    @property
    def path(self) -> str:
        return f"/items/{self.item_id}"

    @property
    def method(self) -> str:
        return "GET"
```

The client method then forwards the request's transformer to `send()`:

```python
async def get_item(self, request: GetItemRequest) -> ItemResponse:
    prepared = await self._base_client.prepare_request(request=request)
    async with self._base_client.send(
        prepared,
        error_transformer=request.error_transformer,
    ) as response:
        return ItemResponse.model_validate(response.json())
```

Callers can override the per-request transformer if they need different error behavior for a specific call.

### Composition rule

When both class- and method-level transformers are configured, the method-level transformer is tried first. If it returns `None`, the class-level transformer is tried. If that also returns `None`, the original `HttpStatusHttpxClientException` is raised.

### `error_transform_context`

`HttpxBaseClient.error_transform_context(error_transformer)` is the seam where transformation actually happens. It wraps `_process_response` in a context manager that catches `HttpStatusHttpxClientException`, runs the method-level transformer, then the class-level transformer, and re-raises the original if both pass. Override on a subclass to wrap the transformation in additional context (e.g. `dl_logging.LogContext`) or hook custom behavior around it:

```python
@contextlib.contextmanager
def error_transform_context(self, error_transformer):
    with dl_logging.LogContext(context={"my.custom.field": ...}):
        with super().error_transform_context(error_transformer):
            yield
```

### Network errors

`RequestHttpxClientException` wraps network-level errors (DNS, connection refused, timeout after all retries). Its `original_exception` attribute holds the underlying error. Network errors are **never** passed through the error transformer mechanism — only `HttpStatusHttpxClientException` (HTTP 4xx/5xx) is.

## Response validation logging

`BaseResponseSchema` overrides `model_validate` and `model_validate_json` to catch `pydantic.ValidationError`, log the raw payload via the `dl_httpx.models.base` logger at `ERROR` level, and re-raise the original exception unchanged. This is the only difference from plain `dl_pydantic.BaseSchema`.

Without this, a `ValidationError` only says *which* field failed validation — not *what* the upstream service actually returned. Logging the raw payload makes incidents like missing/renamed server-side fields actionable from logs alone.

Conventions:

- Inherit any HTTP response model from `BaseResponseSchema`, directly or via mixin.
- When the response shape is shared with the server via an external schema package, do **not** add `dl_httpx` as a dependency of that package. Instead, define a thin client-side subclass that mixes in `BaseResponseSchema`:

  ```python
  import dl_httpx
  import shared_schemas


  class GetItemResponse(shared_schemas.ItemSchema, dl_httpx.BaseResponseSchema): ...
  ```

- The logger is `dl_httpx.models.base`. Configure level/handlers there if you need to suppress (e.g. tests that intentionally feed malformed payloads).
- The raw payload is `repr()`-ed and truncated at 5000 characters (with a `... [truncated, N chars total]` suffix) to keep log lines bounded. Treat the log as potentially sensitive: callers handling tokens or PII should sanitize upstream.

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

### Testing with respx

respx intercepts httpx requests at the transport level. Use the `respx_mock` pytest fixture (auto-provided by the respx plugin) to mock routes.

#### Conftest: client fixture

```python
# my_client_tests/unit/conftest.py
import ssl

import pytest
import pytest_asyncio

import my_client
import dl_auth


BASE_URL = "http://my-service.test"


@pytest.fixture(name="ssl_context")
def fixture_ssl_context() -> ssl.SSLContext:
    return ssl.create_default_context()


@pytest_asyncio.fixture(name="my_async_client")
async def fixture_my_async_client(ssl_context: ssl.SSLContext) -> my_client.MyAsyncClient:
    return my_client.MyAsyncClient.from_dependencies(
        my_client.MyClientDependencies(
            base_url=BASE_URL,
            auth_provider=dl_auth.NoAuthProvider(),
            ssl_context=ssl_context,
        )
    )
```

#### Test: mocking a GET request

```python
# my_client_tests/unit/test_client.py
import pytest
import respx

import my_client

from my_client_tests.unit.conftest import BASE_URL


@pytest.mark.asyncio
async def test_get_items_sends_correct_request(
    my_async_client: my_client.MyAsyncClient,
    respx_mock: respx.MockRouter,
) -> None:
    mock_route = respx_mock.get(f"{BASE_URL}/api/v1/items").respond(
        status_code=200,
        json={"items": [{"id": "1", "name": "Item"}]},
    )

    request = my_client.GetItemsRequest(tenant_id="tenant-123")
    await my_async_client.get_items(request)

    assert mock_route.call_count == 1
    sent = mock_route.calls.last.request
    assert sent.method == "GET"
    assert dict(sent.url.params) == {"tenant_id": "tenant-123"}


@pytest.mark.asyncio
async def test_get_items_returns_parsed_response(
    my_async_client: my_client.MyAsyncClient,
    respx_mock: respx.MockRouter,
) -> None:
    respx_mock.get(f"{BASE_URL}/api/v1/items").respond(
        status_code=200,
        json={"items": [{"id": "abc", "name": "Widget", "status": "active"}]},
    )

    request = my_client.GetItemsRequest(tenant_id="tenant-123")
    response = await my_async_client.get_items(request)

    assert len(response) == 1
    assert response[0].id == "abc"
    assert response[0].name == "Widget"


@pytest.mark.asyncio
async def test_get_items_raises_on_http_error(
    my_async_client: my_client.MyAsyncClient,
    respx_mock: respx.MockRouter,
) -> None:
    respx_mock.get(f"{BASE_URL}/api/v1/items").respond(status_code=500)

    request = my_client.GetItemsRequest(tenant_id="tenant-123")
    with pytest.raises(Exception):
        await my_async_client.get_items(request)
```

#### Key patterns

| Pattern | Code |
|---|---|
| Mock GET | `respx_mock.get(url).respond(status_code=200, json={...})` |
| Mock POST | `respx_mock.post(url).respond(status_code=201, json={...})` |
| Inspect sent request | `mock_route.calls.last.request` |
| Check call count | `assert mock_route.call_count == 1` |
| Check query params | `assert dict(sent.url.params) == {...}` |
| Check headers | `assert sent.headers["Authorization"] == "Bearer ..."` |
| Check body | `import json; assert json.loads(sent.content) == {...}` |
| Simulate error | `respx_mock.get(url).mock(side_effect=httpx.ConnectError(...))` |

#### pyproject.toml test dependencies

```toml
[tool.poetry.group.tests.dependencies]
pytest = "*"
pytest-asyncio = "*"
respx = "*"
```

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
