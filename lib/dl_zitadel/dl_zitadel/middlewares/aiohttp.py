import logging

import aiohttp.typedefs as aiohttp_typedefs
import aiohttp.web as aiohttp_web
import attr

import dl_api_commons.aio.typing as dl_api_commons_aio_typing
import dl_api_commons.aiohttp.aiohttp_wrappers as dl_api_commons_aiohttp_aiohttp_wrappers
import dl_api_commons.base_models as dl_api_commons_base_models
import dl_zitadel.clients as clients
import dl_zitadel.middlewares.models as middlewares_models
import dl_zitadel.services as services


LOGGER = logging.getLogger(__name__)


# TODO: add service user whitelist
# TODO: add no auth routes
@attr.s(auto_attribs=True)
class AioHTTPMiddleware:
    _client: clients.ZitadelAsyncClient
    _token_storage: services.ZitadelAsyncTokenStorage

    def get_middleware(self) -> dl_api_commons_aio_typing.AIOHTTPMiddleware:
        @aiohttp_web.middleware
        @dl_api_commons_aiohttp_aiohttp_wrappers.DLRequestBase.use_dl_request
        async def inner(
            app_request: dl_api_commons_aiohttp_aiohttp_wrappers.DLRequestBase,
            handler: aiohttp_typedefs.Handler,
        ) -> aiohttp_web.StreamResponse:
            service_access_token = app_request.get_single_header(middlewares_models.ZitadelHeaders.SERVICE_ACCESS_TOKEN)
            if service_access_token is None:
                LOGGER.info("Service access token is missing")
                raise aiohttp_web.HTTPUnauthorized()

            user_access_token = app_request.get_single_header(
                dl_api_commons_base_models.DLHeadersCommon.AUTHORIZATION_TOKEN
            )
            if user_access_token is None:
                LOGGER.info("User access token is missing")
                raise aiohttp_web.HTTPUnauthorized()

            token_prefix = "Bearer "
            assert user_access_token.startswith(token_prefix)
            user_access_token = user_access_token[len(token_prefix) :]

            # TODO: add gather to introspect in parallel
            service_introspect_result = await self._client.introspect(token=service_access_token)

            if not service_introspect_result.active:
                LOGGER.info("Service access token is not active")
                raise aiohttp_web.HTTPUnauthorized()

            user_introspect_result = await self._client.introspect(token=user_access_token)

            if not user_introspect_result.active:
                LOGGER.info("User access token is not active")
                raise aiohttp_web.HTTPUnauthorized()

            app_request.replace_temp_rci(
                attr.evolve(
                    app_request.temp_rci,
                    user_id=user_introspect_result.sub,
                    user_name=user_introspect_result.username,
                    auth_data=middlewares_models.ZitadelAuthData(
                        service_access_token=await self._token_storage.get_token(),
                        user_access_token=user_access_token,
                    ),
                )
            )

            return await handler(app_request.request)

        return inner


__all__ = [
    "AioHTTPMiddleware",
]
