"""
Middleware to reset asyncio event loop on each request
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

import attr

from dl_api_commons.flask.middlewares.wsgi_middleware import FlaskWSGIMiddleware


if TYPE_CHECKING:
    from dl_api_commons.flask.types import (
        WSGIEnviron,
        WSGIReturn,
        WSGIStartResponse,
    )


@attr.s
class AIOEventLoopMiddleware(FlaskWSGIMiddleware):
    _APP_FLAG_ATTR_NAME = "_bi_middleware_aio_event_loop"

    debug: bool = attr.ib(default=False)  # If `True` - debug mode for event loop will be turned on

    def wsgi_app(self, environ: WSGIEnviron, start_response: WSGIStartResponse) -> WSGIReturn:
        loop = asyncio.new_event_loop()
        loop.set_debug(self.debug)
        asyncio.set_event_loop(loop)

        try:
            return self.original_wsgi_app(environ, start_response)
        finally:
            loop.close()
            asyncio.set_event_loop(None)
