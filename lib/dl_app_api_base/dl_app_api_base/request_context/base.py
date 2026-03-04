import contextvars
import logging
from typing import (
    Generic,
    Mapping,
    Protocol,
    TypeVar,
)

import aiohttp.web
import attr
from typing_extensions import Self


LOGGER = logging.getLogger(__name__)


@attr.define(kw_only=True, slots=False)
class BaseRequestContextDependencies:
    ...


_RequestContextDependenciesContravariantType = TypeVar(
    "_RequestContextDependenciesContravariantType",
    bound=BaseRequestContextDependencies,
    contravariant=True,
)


@attr.define(kw_only=True, slots=False)
class BaseRequestContext(Generic[_RequestContextDependenciesContravariantType]):
    _aiohttp_request: aiohttp.web.Request
    _dependencies: _RequestContextDependenciesContravariantType

    @property
    def method(self) -> str:
        return self._aiohttp_request.method

    @property
    def path(self) -> str:
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
        dependencies: _RequestContextDependenciesContravariantType,
    ) -> Self:
        return cls(
            aiohttp_request=aiohttp_request,
            dependencies=dependencies,
        )


_RequestContextCovariantType = TypeVar("_RequestContextCovariantType", bound=BaseRequestContext, covariant=True)


class RequestContextFactoryProtocol(
    Protocol[
        _RequestContextDependenciesContravariantType,
        _RequestContextCovariantType,
    ]
):
    def __call__(
        self,
        aiohttp_request: aiohttp.web.Request,
        dependencies: _RequestContextDependenciesContravariantType,
    ) -> _RequestContextCovariantType:
        ...


def _get_context_var() -> contextvars.ContextVar[_RequestContextCovariantType | None]:
    return contextvars.ContextVar("dl_app_api_base.request_context", default=None)


@attr.define(frozen=True, kw_only=True)
class RequestContextProvider(Generic[_RequestContextCovariantType]):
    _context_var: contextvars.ContextVar[_RequestContextCovariantType | None] = attr.ib(
        factory=_get_context_var,
        init=False,
    )

    @property
    def context_var(self) -> contextvars.ContextVar[_RequestContextCovariantType | None]:
        return self._context_var

    def get(self) -> _RequestContextCovariantType:
        context = self._context_var.get()
        if context is None:
            raise ValueError("Context is not set")
        return context


@attr.define(frozen=True, kw_only=True)
class BaseRequestContextManager(Generic[_RequestContextDependenciesContravariantType, _RequestContextCovariantType]):
    _context_factory: RequestContextFactoryProtocol[
        _RequestContextDependenciesContravariantType,
        _RequestContextCovariantType,
    ]
    _dependencies: _RequestContextDependenciesContravariantType
    _context_var: contextvars.ContextVar[_RequestContextCovariantType | None]

    def create(self, aiohttp_request: aiohttp.web.Request) -> _RequestContextCovariantType:
        request_context = self._context_factory(
            aiohttp_request=aiohttp_request,
            dependencies=self._dependencies,
        )
        self._context_var.set(request_context)
        return request_context

    def clear(self) -> None:
        if self._context_var.get() is None:
            raise ValueError("Context is not set")

        self._context_var.set(None)


class RequestContextProviderProtocol(Protocol[_RequestContextCovariantType]):
    def get(self) -> _RequestContextCovariantType:
        ...


class RequestContextManagerProtocol(Protocol[_RequestContextCovariantType]):
    def create(self, aiohttp_request: aiohttp.web.Request) -> _RequestContextCovariantType:
        ...

    def clear(self) -> None:
        ...
