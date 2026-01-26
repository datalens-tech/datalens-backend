import http
import logging

import aiohttp.typedefs
import aiohttp.web
import attr

import dl_app_api_base.handlers


LOGGER = logging.getLogger(__name__)


@attr.define(frozen=True, kw_only=True)
class ErrorHandlingMiddleware:
    @aiohttp.web.middleware
    async def process(
        self,
        request: aiohttp.web.Request,
        handler: aiohttp.typedefs.Handler,
    ) -> aiohttp.web.StreamResponse:
        try:
            return await handler(request)
        except aiohttp.web.HTTPNotFound:
            return dl_app_api_base.handlers.Response.with_error(
                message="Not found",
                code="NOT_FOUND",
                status=http.HTTPStatus.NOT_FOUND,
            )
        except Exception:
            # TODO: add error handling mechanism here
            LOGGER.exception("Failed to handle request error")
            return dl_app_api_base.handlers.Response.with_error(
                message="Internal server error",
                code="INTERNAL_SERVER_ERROR",
                status=http.HTTPStatus.INTERNAL_SERVER_ERROR,
            )
