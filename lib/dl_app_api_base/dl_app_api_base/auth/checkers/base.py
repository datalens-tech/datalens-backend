import abc
import logging
from typing import (
    Protocol,
    Sequence,
)

import aiohttp.web
import attr

import dl_app_api_base.auth.models as auth_models


LOGGER = logging.getLogger(__name__)


@attr.define(frozen=True, kw_only=True)
class BaseRequestAuthResult:
    ...


class RequestAuthCheckerProtocol(Protocol):
    async def is_applicable(self, request: aiohttp.web.Request) -> bool:
        ...

    async def check(self, request: aiohttp.web.Request) -> BaseRequestAuthResult:
        """
        :raises: AuthFailureError if the authentication fails
        """
        ...


@attr.define(frozen=True, kw_only=True)
class BaseRequestAuthChecker(abc.ABC):
    _route_matchers: Sequence[auth_models.RouteMatcher]

    async def is_applicable(self, request: aiohttp.web.Request) -> bool:
        return any(route_matcher.matches(request.path, request.method) for route_matcher in self._route_matchers)

    @abc.abstractmethod
    async def check(self, request: aiohttp.web.Request) -> BaseRequestAuthResult:
        ...
