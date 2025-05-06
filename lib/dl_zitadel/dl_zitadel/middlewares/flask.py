import logging

import attr
import flask
import werkzeug.exceptions as werkzeug_exceptions

import dl_api_commons.base_models as dl_api_commons_base_models
import dl_api_commons.flask.middlewares.commit_rci_middleware as dl_api_commons_flask_rci
from dl_api_commons.flask.required_resources import (
    RequiredResourceCommon,
    get_required_resources,
)
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

    def process(self) -> flask.Response | None:
        # TODO BI-6257 move tenant resolution into a separate middleware
        temp_rci = dl_api_commons_flask_rci.ReqCtxInfoMiddleware.get_temp_rci()
        dl_api_commons_flask_rci.ReqCtxInfoMiddleware.replace_temp_rci(
            temp_rci.clone(
                tenant=dl_api_commons_base_models.TenantCommon(),
            )
        )

        required_resources = get_required_resources()

        if RequiredResourceCommon.SKIP_AUTH in required_resources:
            LOGGER.info("Auth was skipped due to SKIP_AUTH flag in target view")
            return None

        auth_result = self.auth()

        if auth_result is None:
            raise werkzeug_exceptions.Unauthorized()

        temp_rci = dl_api_commons_flask_rci.ReqCtxInfoMiddleware.get_temp_rci()
        dl_api_commons_flask_rci.ReqCtxInfoMiddleware.replace_temp_rci(
            temp_rci.clone(
                user_id=auth_result.user_id,
                user_name=auth_result.user_name,
                auth_data=auth_result.data,
            )
        )

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

        try:
            access_token = zitadel_cookie["passport"]["user"]["accessToken"]
        except KeyError:
            access_token = None

        if access_token is None:
            LOGGER.info("Access token is missing")
            return None

        token_introspect_result = self._client.introspect(token=access_token)

        if not token_introspect_result.active:
            LOGGER.info("Access token is not active")
            return None

        LOGGER.info("Access token is active")

        return middlewares_models.AuthResult(
            user_id=token_introspect_result.sub,
            user_name=token_introspect_result.username,
            data=middlewares_models.ZitadelAuthData(
                service_access_token=self._token_storage.get_token(),
                user_access_token=access_token,
            ),
            updated_cookies={},
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
