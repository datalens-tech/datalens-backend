import logging

import aiohttp.typedefs as aiohttp_typedefs
import aiohttp.web as aiohttp_web
import attr

import dl_api_commons.aio.typing as dl_api_commons_aio_typing
import dl_api_commons.aiohttp.aiohttp_wrappers as dl_api_commons_aiohttp_aiohttp_wrappers
import dl_auth_native.middlewares.base as middlewares_base


LOGGER = logging.getLogger(__name__)


@attr.s
class AioHTTPMiddleware(middlewares_base.BaseMiddleware):
    def get_middleware(self) -> dl_api_commons_aio_typing.AIOHTTPMiddleware:
        @aiohttp_web.middleware
        @dl_api_commons_aiohttp_aiohttp_wrappers.DLRequestBase.use_dl_request
        async def inner(
            app_request: dl_api_commons_aiohttp_aiohttp_wrappers.DLRequestBase,
            handler: aiohttp_typedefs.Handler,
        ) -> aiohttp_web.StreamResponse:
            user_access_token_header = app_request.get_single_header(self._user_access_header_key)

            try:
                auth_result = self._auth(user_access_token_header)
            except self.Unauthorized as exc:
                LOGGER.info(f"Unauthorized: {exc.message}")
                raise aiohttp_web.HTTPUnauthorized(reason=exc.message) from exc

            app_request.replace_temp_rci(
                app_request.temp_rci.clone(
                    user_id=auth_result.user_id,
                    tenant=auth_result.tenant,
                    auth_data=auth_result.auth_data,
                )
            )
            return await handler(app_request.request)

        return inner


__all__ = [
    "AioHTTPMiddleware",
]
