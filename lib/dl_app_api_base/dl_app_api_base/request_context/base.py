import contextvars
import logging
from typing import (
    Generic,
    Protocol,
    TypeVar,
)

import aiohttp.web
import attr
from typing_extensions import Self


LOGGER = logging.getLogger(__name__)
CONTEXT_VAR_NAME = "dl_app_api_base.request_context"


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


def _create_context_var() -> contextvars.ContextVar[_RequestContextCovariantType | None]:
    return contextvars.ContextVar(CONTEXT_VAR_NAME, default=None)


@attr.define(frozen=True, kw_only=True)
class BaseRequestContextManager(Generic[_RequestContextDependenciesContravariantType, _RequestContextCovariantType]):
    _context_factory: RequestContextFactoryProtocol[
        _RequestContextDependenciesContravariantType,
        _RequestContextCovariantType,
    ]
    _dependencies: _RequestContextDependenciesContravariantType
    _context_var: contextvars.ContextVar[_RequestContextCovariantType | None] = attr.ib(
        factory=_create_context_var,
        init=False,
    )

    def is_set(self) -> bool:
        return self._context_var.get() is not None

    def get(self) -> _RequestContextCovariantType:
        context = self._context_var.get()
        if context is None:
            raise ValueError("Context is not set")
        return context

    def create(self, aiohttp_request: aiohttp.web.Request) -> _RequestContextCovariantType:
        request_context = self._create(aiohttp_request)
        self._context_var.set(request_context)
        return request_context

    def _create(self, aiohttp_request: aiohttp.web.Request) -> _RequestContextCovariantType:
        return self._context_factory(
            aiohttp_request=aiohttp_request,
            dependencies=self._dependencies,
        )

    def clear(self) -> None:
        if not self.is_set():
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
