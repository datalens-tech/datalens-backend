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
import dl_auth_native.middlewares.base as middlewares_base


LOGGER = logging.getLogger(__name__)


@attr.s()
class FlaskMiddleware(middlewares_base.BaseMiddleware):
    def set_up(self, app: flask.Flask) -> None:
        app.before_request(self.process)

    def process(self) -> None:
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

        user_access_token_header = flask.request.headers.get(self._user_access_header_key)

        try:
            auth_result = self._auth(user_access_token_header)
        except self.Unauthorized as exc:
            LOGGER.info(f"Unauthorized: {exc.message}")
            raise werkzeug_exceptions.Unauthorized(description=exc.message) from exc

        temp_rci = dl_api_commons_flask_rci.ReqCtxInfoMiddleware.get_temp_rci()
        dl_api_commons_flask_rci.ReqCtxInfoMiddleware.replace_temp_rci(
            temp_rci.clone(
                user_id=auth_result.user_id,
                tenant=auth_result.tenant,
                auth_data=auth_result.auth_data,
            )
        )

        return None


__all__ = [
    "FlaskMiddleware",
]
