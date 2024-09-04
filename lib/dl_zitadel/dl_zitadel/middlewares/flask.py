import logging
from typing import Optional

import attr
import flask
from flask import g as flask_context
import werkzeug.exceptions as werkzeug_exceptions

import dl_api_commons.base_models as dl_api_commons_base_models
import dl_api_commons.flask.middlewares.commit_rci_middleware as dl_api_commons_flask_rci
import dl_zitadel.clients as clients
import dl_zitadel.middlewares.models as middlewares_models
import dl_zitadel.services as services


LOGGER = logging.getLogger(__name__)


# TODO: add service user whitelist
# TODO: add no auth routes
@attr.s(auto_attribs=True)
class FlaskMiddleware:
    _client: clients.ZitadelSyncClient
    _token_storage: services.ZitadelSyncTokenStorage
    _allow_user_auth: bool = attr.ib(default=False)
    _allow_service_auth: bool = attr.ib(default=True)

    def set_up(self, app: flask.Flask) -> None:
        app.before_request(self.process)
        app.teardown_request(self.process_response)

    def process_response(self, exception: Optional[BaseException]) -> None:
        if self._allow_user_auth:
            for key, value in flask_context.get("cookies", {}).items():
                flask.response.set_cookie(key, value)

        return None

    def process(self) -> flask.Response | None:
        auth_result = self.auth()

        if auth_result is None:
            raise werkzeug_exceptions.Unauthorized()

        temp_rci = dl_api_commons_flask_rci.ReqCtxInfoMiddleware.get_temp_rci()
        dl_api_commons_flask_rci.ReqCtxInfoMiddleware.replace_temp_rci(
            temp_rci.clone(
                user_id=auth_result.user_id,
                user_name=auth_result.user_name,
                tenant=dl_api_commons_base_models.TenantCommon(),
                auth_data=auth_result.data,
            )
        )
        flask_context.cookies = auth_result.updated_cookies

        return None

    def auth(self) -> middlewares_models.AuthResult | None:
        result = None

        if result is None and self._allow_user_auth:
            result = self.auth_user()

        if result is None and self._allow_service_auth:
            result = self.auth_service()

        return result

    def auth_user(self) -> middlewares_models.AuthResult | None:
        zitadel_cookie = middlewares_models.ZitadelCookiesParser.from_raw(
            flask.request.cookies.get(middlewares_models.ZitadelCookies.ZITADEL_COOKIE)
        )

        if zitadel_cookie is None:
            LOGGER.info("Zitadel cookie is missing")
            return None

        flask_context.zitadel_cookie = zitadel_cookie

        try:
            access_token, refresh_token = (
                zitadel_cookie["passport"]["user"]["accessToken"],
                zitadel_cookie["passport"]["user"]["refreshToken"],
            )
        except KeyError:
            access_token, refresh_token = None, None

        if access_token is None:
            LOGGER.info("Access token is missing")
            return None

        token_introspect_result = self._client.introspect(token=access_token)

        if not token_introspect_result.active:
            LOGGER.info("Access token is not active")

            if refresh_token is None:
                LOGGER.info("Refresh token is missing")
                return None

            refreshed_tokens = self._client.refresh_token(refresh_token)
            LOGGER.info("Tokens successfully refreshed")
            access_token, refresh_token = (
                refreshed_tokens.access_token,
                refreshed_tokens.refresh_token,
            )

            token_introspect_result = self._client.introspect(token=access_token)

        else:
            LOGGER.info("Access token is active")

        zitadel_cookie["passport"]["user"]["accessToken"] = access_token
        zitadel_cookie["passport"]["user"]["refreshToken"] = refresh_token

        zitadel_cookies = middlewares_models.ZitadelCookiesParser.to_raw(zitadel_cookie)

        return middlewares_models.AuthResult(
            user_id=token_introspect_result.sub,
            user_name=token_introspect_result.username,
            data=middlewares_models.ZitadelAuthData(
                service_access_token=self._token_storage.get_token(),
                user_access_token=access_token,
            ),
            updated_cookies={middlewares_models.ZitadelCookies.ZITADEL_COOKIE: zitadel_cookies},
        )

    def auth_service(self) -> middlewares_models.AuthResult | None:
        service_access_token = flask.request.headers.get(middlewares_models.ZitadelHeaders.SERVICE_ACCESS_TOKEN)
        user_access_token = flask.request.headers.get(dl_api_commons_base_models.DLHeadersCommon.AUTHORIZATION_TOKEN)

        if service_access_token is None:
            LOGGER.info("Service access token is missing")
            return None

        if user_access_token is None:
            LOGGER.info("User access token is missing")
            return None

        token_prefix = "Bearer "
        assert user_access_token.startswith(token_prefix)
        user_access_token = user_access_token[len(token_prefix) :]

        service_introspect_result = self._client.introspect(token=service_access_token)

        if not service_introspect_result.active:
            LOGGER.info("Service access token is not active")
            return None

        user_introspect_result = self._client.introspect(token=user_access_token)

        if not user_introspect_result.active:
            LOGGER.info("User access token is not active")
            return None

        return middlewares_models.AuthResult(
            user_id=user_introspect_result.sub,
            user_name=user_introspect_result.username,
            data=middlewares_models.ZitadelAuthData(
                service_access_token=self._token_storage.get_token(),
                user_access_token=user_access_token,
            ),
            updated_cookies={},
        )


__all__ = [
    "FlaskMiddleware",
]
