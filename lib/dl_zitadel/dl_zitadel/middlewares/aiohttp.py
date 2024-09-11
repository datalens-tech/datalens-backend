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
    _allow_user_auth: bool = attr.ib(default=False)
    _allow_service_auth: bool = attr.ib(default=True)

    async def auth_user(
        self,
        app_request: dl_api_commons_aiohttp_aiohttp_wrappers.DLRequestBase,
    ) -> middlewares_models.AuthResult | None:
        req = app_request.request

        zitadel_cookie = middlewares_models.ZitadelCookiesParser.from_raw(
            req.cookies.get(middlewares_models.ZitadelCookies.ZITADEL_COOKIE)
        )

        if zitadel_cookie is None:
            LOGGER.info("Zitadel cookie is missing")
            return None

        try:
            access_token = zitadel_cookie["passport"]["user"]["accessToken"]
        except KeyError:
            access_token = None

        if access_token is None:
            LOGGER.info("Access token is missing")
            return None

        token_introspect_result = await self._client.introspect(token=access_token)

        if not token_introspect_result.active:
            LOGGER.info("Access token is not active")
            return None

        LOGGER.info("Access token is active")

        return middlewares_models.AuthResult(
            user_id=token_introspect_result.sub,
            user_name=token_introspect_result.username,
            data=middlewares_models.ZitadelAuthData(
                service_access_token=await self._token_storage.get_token(),
                user_access_token=access_token,
            ),
            updated_cookies={},
        )

    async def auth_service(
        self,
        app_request: dl_api_commons_aiohttp_aiohttp_wrappers.DLRequestBase,
    ) -> middlewares_models.AuthResult | None:
        service_access_token = app_request.get_single_header(middlewares_models.ZitadelHeaders.SERVICE_ACCESS_TOKEN)
        user_access_token = app_request.get_single_header(
            dl_api_commons_base_models.DLHeadersCommon.AUTHORIZATION_TOKEN
        )
        if service_access_token is None:
            LOGGER.info("Service access token is missing")
            return None

        if user_access_token is None:
            LOGGER.info("User access token is missing")
            return None

        token_prefix = "Bearer "
        assert user_access_token.startswith(token_prefix)
        user_access_token = user_access_token[len(token_prefix) :]

        # TODO: add gather to introspect in parallel
        service_introspect_result = await self._client.introspect(token=service_access_token)

        if not service_introspect_result.active:
            LOGGER.info("Service access token is not active")
            return None

        user_introspect_result = await self._client.introspect(token=user_access_token)

        if not user_introspect_result.active:
            LOGGER.info("User access token is not active")
            return None

        return middlewares_models.AuthResult(
            user_id=user_introspect_result.sub,
            user_name=user_introspect_result.username,
            data=middlewares_models.ZitadelAuthData(
                service_access_token=await self._token_storage.get_token(),
                user_access_token=user_access_token,
            ),
            updated_cookies={},
        )

    async def auth(
        self, app_request: dl_api_commons_aiohttp_aiohttp_wrappers.DLRequestBase
    ) -> middlewares_models.AuthResult | None:
        result = None

        if result is None and self._allow_user_auth:
            result = await self.auth_user(app_request)

        if result is None and self._allow_service_auth:
            result = await self.auth_service(app_request)

        return result

    def get_middleware(self) -> dl_api_commons_aio_typing.AIOHTTPMiddleware:
        @aiohttp_web.middleware
        @dl_api_commons_aiohttp_aiohttp_wrappers.DLRequestBase.use_dl_request
        async def inner(
            app_request: dl_api_commons_aiohttp_aiohttp_wrappers.DLRequestBase,
            handler: aiohttp_typedefs.Handler,
        ) -> aiohttp_web.StreamResponse:
            auth_result = await self.auth(app_request)

            if auth_result is None:
                raise aiohttp_web.HTTPUnauthorized()

            app_request.replace_temp_rci(
                attr.evolve(
                    app_request.temp_rci,
                    user_id=auth_result.user_id,
                    user_name=auth_result.user_name,
                    tenant=dl_api_commons_base_models.TenantCommon(),
                    auth_data=auth_result.data,
                )
            )
            response = await handler(app_request.request)

            for key, value in auth_result.updated_cookies.items():
                response.set_cookie(key, value)

            return response

        return inner


__all__ = [
    "AioHTTPMiddleware",
]
