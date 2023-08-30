from __future__ import annotations

from typing import Any, Callable, Awaitable

from aiohttp import web
from aiohttp.typedefs import Handler

# TODO: replace AIOHTTPMiddleware with aiohttp.typedefs.Middleware after it's released and updated in Arcadia
# https://github.com/aio-libs/aiohttp/pull/5898
AIOHTTPMiddleware = Callable[[web.Request, Handler], Awaitable[web.StreamResponse]]
AIOHTTPMethodMiddleware = Callable[[Any, web.Request, Handler], Awaitable[web.StreamResponse]]
