from __future__ import annotations

import logging
from typing import Optional

import flask
from flask import request
from flask_restx import Api
from werkzeug.exceptions import (
    BadRequest,
    UnavailableForLegalReasons,
)

from dl_api_commons.access_control_common import AuthFailureError
from dl_api_commons.flask.middlewares.commit_rci_middleware import ReqCtxInfoMiddleware
from dl_core.exc import USReqException


API = Api(prefix="/api/v1")

LOGGER = logging.getLogger(__name__)


def init_apis(app: flask.Flask) -> None:
    from dl_api_lib.app.control_api.resources import connections  # noqa
    from dl_api_lib.app.control_api.resources import dataset  # noqa
    from dl_api_lib.app.control_api.resources import info  # noqa
    from dl_api_lib.app.control_api.resources import monitoring  # noqa

    API.init_app(app)


@API.errorhandler(USReqException)
def handle_us_error(error):  # type: ignore  # 2024-01-30 # TODO: Function is missing a type annotation  [no-untyped-def]
    resp = error.orig_exc.response
    try:
        text = resp.text
    except Exception:
        text = resp.content.decode("utf-8", errors="replace")
    LOGGER.warning(
        "Handling US error: %r (%s)",
        text,
        resp.status_code,
    )
    return {"message": text}, resp.status_code


@API.errorhandler(UnavailableForLegalReasons)
def handle_us_read_only_mode_error(error):  # type: ignore  # 2024-01-30 # TODO: Function is missing a type annotation  [no-untyped-def]
    # flask_restx doesn't support HTTP 451, so we have to handle it manually
    return error.data, 451


@API.errorhandler(BadRequest)
def handle_bad_request(error):  # type: ignore  # 2024-01-30 # TODO: Function is missing a type annotation  [no-untyped-def]
    rci = ReqCtxInfoMiddleware.get_last_resort_rci()
    req_id: Optional[str] = rci.request_id if rci is not None else None

    LOGGER.warning(
        "Bad request on %s: %s, req_id: %s",
        request.url,
        error.data,
        req_id,
    )
    return error.data, 400


@API.errorhandler(AuthFailureError)
def handle_auth_error(error):  # type: ignore  # 2024-01-24 # TODO: Function is missing a type annotation  [no-untyped-def]
    if error.response_code in (401, 403):
        if error.user_message is None:
            LOGGER.warning("No user message for AuthFailureError with defined response code", exc_info=True)
            user_message = "Auth failure"
        else:
            user_message = error.user_message
        http_response_code = error.response_code
    else:
        user_message = "Internal server error"
        http_response_code = 500

    return {"message": user_message}, http_response_code
