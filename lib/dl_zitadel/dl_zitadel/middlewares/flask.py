import logging

import attr
import flask
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

    def set_up(self, app: flask.Flask) -> None:
        app.before_request(self.process)

    def process(self) -> flask.Response | None:
        service_access_token = flask.request.headers.get(middlewares_models.ZitadelHeaders.SERVICE_ACCESS_TOKEN)
        user_access_token = flask.request.headers.get(dl_api_commons_base_models.DLHeadersCommon.AUTHORIZATION_TOKEN)

        token_prefix = "Bearer "
        assert user_access_token.startswith(token_prefix)
        user_access_token = user_access_token[len(token_prefix):]

        if service_access_token is None or user_access_token is None:
            LOGGER.info("Service or user access token is missing")
            raise werkzeug_exceptions.Unauthorized()

        service_introspect_result = self._client.introspect(token=service_access_token)

        if not service_introspect_result.active:
            LOGGER.info("Service access token is not active")
            raise werkzeug_exceptions.Unauthorized()

        user_introspect_result = self._client.introspect(token=user_access_token)

        if not user_introspect_result.active:
            LOGGER.info("User access token is not active")
            raise werkzeug_exceptions.Unauthorized()

        temp_rci = dl_api_commons_flask_rci.ReqCtxInfoMiddleware.get_temp_rci()
        dl_api_commons_flask_rci.ReqCtxInfoMiddleware.replace_temp_rci(
            temp_rci.clone(
                user_id=user_introspect_result.sub,
                user_name=user_introspect_result.username,
                auth_data=middlewares_models.ZitadelAuthData(
                    service_access_token=self._token_storage.get_token(),
                    user_access_token=user_access_token,
                ),
            )
        )

        return None


__all__ = [
    "FlaskMiddleware",
]
