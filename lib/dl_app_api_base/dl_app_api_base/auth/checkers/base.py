import abc
import logging
from typing import (
    Protocol,
    Sequence,
)

import attr

import dl_app_api_base.auth.models as auth_models
import dl_app_api_base.request_context as request_context


LOGGER = logging.getLogger(__name__)


@attr.define(frozen=True, kw_only=True)
class BaseRequestAuthResult:
    ...


class RequestAuthCheckerProtocol(Protocol):
    async def is_applicable(self) -> bool:
        ...

    async def check(self) -> BaseRequestAuthResult:
        """
        :raises: AuthFailureError if the authentication fails
        """
        ...


@attr.define(frozen=True, kw_only=True)
class BaseRequestAuthChecker(abc.ABC):
    _route_matchers: Sequence[auth_models.RouteMatcher]
    _context_provider: request_context.RequestContextProviderProtocol[request_context.BaseRequestContext]

    async def is_applicable(self) -> bool:
        context = self._context_provider.get()
        return any(route_matcher.matches(context.path, context.method) for route_matcher in self._route_matchers)

    @abc.abstractmethod
    async def check(self) -> BaseRequestAuthResult:
        ...
