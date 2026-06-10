from collections.abc import Mapping
import contextvars
import logging
from typing import (
    Protocol,
    Self,
)

import aiohttp.web
import attr

LOGGER = logging.getLogger(__name__)


@attr.define(kw_only=True, slots=False)
class BaseRequestContextDependencies: ...


@attr.define(kw_only=True, slots=False)
class BaseRequestContext[RequestContextDependenciesT: BaseRequestContextDependencies]:
    _aiohttp_request: aiohttp.web.Request
    _dependencies: RequestContextDependenciesT

    @property
    def method(self) -> str:
        return self._aiohttp_request.method

    @property
    def path(self) -> str:
        return self._aiohttp_request.path

    @property
    def raw_path(self) -> str:
        return self._aiohttp_request.raw_path

    @property
    def path_pattern(self) -> str:
        resource = self._aiohttp_request.match_info.route.resource
        if resource is None:
            return self._aiohttp_request.path
        return resource.canonical

    @property
    def headers(self) -> Mapping[str, str]:
        return self._aiohttp_request.headers

    @property
    def cookies(self) -> Mapping[str, str]:
        return self._aiohttp_request.cookies

    @property
    def host(self) -> str:
        return self._aiohttp_request.host

    @classmethod
    def factory(
        cls,
        aiohttp_request: aiohttp.web.Request,
        dependencies: RequestContextDependenciesT,
    ) -> Self:
        return cls(
            aiohttp_request=aiohttp_request,
            dependencies=dependencies,
        )


class RequestContextFactoryProtocol[
    RequestContextDependenciesT: BaseRequestContextDependencies,
    RequestContextT: BaseRequestContext,
](Protocol):
    def __call__(
        self,
        aiohttp_request: aiohttp.web.Request,
        dependencies: RequestContextDependenciesT,
    ) -> RequestContextT: ...


def _create_context_var[RequestContextT: BaseRequestContext]() -> contextvars.ContextVar[RequestContextT | None]:
    return contextvars.ContextVar("dl_app_api_base.request_context", default=None)


@attr.define(frozen=True, kw_only=True)
class RequestContextVar[RequestContextT: BaseRequestContext]:
    """Thin typed wrapper around `contextvars.ContextVar` holding the current request context."""

    _context_var: contextvars.ContextVar[RequestContextT | None] = attr.field(
        factory=_create_context_var,
        init=False,
    )

    def get(self) -> RequestContextT | None:
        return self._context_var.get()

    def set(self, request_context: RequestContextT) -> None:
        self._context_var.set(request_context)

    def clear(self) -> None:
        self._context_var.set(None)


@attr.define(frozen=True, kw_only=True)
class RequestContextProvider[RequestContextT: BaseRequestContext]:
    _context_var: RequestContextVar[RequestContextT]

    def get(self) -> RequestContextT:
        context = self._context_var.get()
        if context is None:
            raise ValueError("Context is not set")
        return context


@attr.define(frozen=True, kw_only=True)
class BaseRequestContextManager[
    RequestContextDependenciesT: BaseRequestContextDependencies,
    RequestContextT: BaseRequestContext,
]:
    _context_factory: RequestContextFactoryProtocol[
        RequestContextDependenciesT,
        RequestContextT,
    ]
    _dependencies: RequestContextDependenciesT
    _context_var: RequestContextVar[RequestContextT]

    def create(self, aiohttp_request: aiohttp.web.Request) -> RequestContextT:
        request_context = self._context_factory(
            aiohttp_request=aiohttp_request,
            dependencies=self._dependencies,
        )
        self._context_var.set(request_context)
        return request_context

    def clear(self) -> None:
        if self._context_var.get() is None:
            raise ValueError("Context is not set")

        self._context_var.clear()


class RequestContextProviderProtocol[RequestContextT: BaseRequestContext](Protocol):
    def get(self) -> RequestContextT: ...


class RequestContextManagerProtocol[RequestContextT: BaseRequestContext](Protocol):
    def create(self, aiohttp_request: aiohttp.web.Request) -> RequestContextT: ...

    def clear(self) -> None: ...
