from __future__ import annotations

from typing import (
    Any,
    Awaitable,
    Callable,
)

from aiohttp import web
from aiohttp.typedefs import Handler

# TODO: replace AIOHTTPMiddleware with aiohttp.typedefs.Middleware
AIOHTTPMiddleware = Callable[[web.Request, Handler], Awaitable[web.StreamResponse]]
AIOHTTPMethodMiddleware = Callable[[Any, web.Request, Handler], Awaitable[web.StreamResponse]]
