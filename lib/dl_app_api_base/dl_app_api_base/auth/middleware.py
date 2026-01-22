import http
import logging

import aiohttp.typedefs
import aiohttp.web
import attr

import dl_app_api_base.auth.exc as auth_exc
import dl_app_api_base.auth.request_context as auth_request_context
import dl_app_api_base.handlers as handlers
import dl_app_api_base.request_context as request_context


LOGGER = logging.getLogger(__name__)


@attr.define(frozen=True, kw_only=True)
class AuthMiddleware:
    _request_context_provider: request_context.RequestContextProviderProtocol[
        auth_request_context.AuthRequestContextMixin
    ]

    @aiohttp.web.middleware
    async def process(
        self,
        request: aiohttp.web.Request,
        handler: aiohttp.typedefs.Handler,
    ) -> aiohttp.web.StreamResponse:
        context = self._request_context_provider.get()
        try:
            await context.get_auth_user()
        except auth_exc.AuthError:
            LOGGER.exception("Authentication failed")
            return handlers.Response.with_error(
                message="Unauthorized",
                code="UNAUTHORIZED",
                status=http.HTTPStatus.UNAUTHORIZED,
            )

        return await handler(request)
