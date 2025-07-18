import logging

import aiohttp.typedefs as aiohttp_typedefs
import aiohttp.web as aiohttp_web
import attr

import dl_api_commons.aiohttp.aiohttp_wrappers as dl_api_commons_aiohttp_aiohttp_wrappers
from dl_api_commons.aiohttp.required_resources import (
    RequiredResourceCommon,
    get_required_resources,
)
import dl_auth_native.middlewares.base as middlewares_base


LOGGER = logging.getLogger(__name__)


@attr.s
class AioHTTPMiddleware(middlewares_base.BaseMiddleware):
    def get_middleware(self) -> aiohttp_typedefs.Middleware:
        @aiohttp_web.middleware
        @dl_api_commons_aiohttp_aiohttp_wrappers.DLRequestBase.use_dl_request
        async def inner(
            app_request: dl_api_commons_aiohttp_aiohttp_wrappers.DLRequestBase,
            handler: aiohttp_typedefs.Handler,
        ) -> aiohttp_web.StreamResponse:
            required_resources = get_required_resources(app_request)

            if RequiredResourceCommon.SKIP_AUTH in required_resources:
                LOGGER.info("Auth was skipped due to SKIP_AUTH flag in target view")
                return await handler(app_request.request)

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
