from __future__ import annotations

import logging

import flask
from typing import Type, Callable, Any, Optional
from werkzeug.exceptions import BadRequest
from flask import request
from flask_restx import Api

from bi_api_commons.flask.middlewares.commit_rci_middleware import ReqCtxInfoMiddleware
from bi_core.exc import USReqException


API = Api(prefix='/api/v1')

LOGGER = logging.getLogger(__name__)


def init_apis(
        app: flask.Flask,
        map_exc_err_handler: dict[Type[Exception], Callable[[Any], Any]],
) -> None:
    from . import dataset  # noqa
    from . import info  # noqa
    from . import connections  # noqa
    API.init_app(app)
    for exc_type, handler in map_exc_err_handler.items():
        API.errorhandler(exc_type)(handler)


@API.errorhandler(USReqException)
def handle_us_error(error):  # type: ignore  # TODO: fix
    resp = error.orig_exc.response
    try:
        text = resp.text
    except Exception:
        text = resp.content.decode('utf-8', errors='replace')
    LOGGER.warning(
        'Handling US error: %r (%s) (folder %s)',
        text,
        resp.status_code,
        request.headers.get('X-YaCloud-FolderId'),
    )
    return {'message': text}, resp.status_code


@API.errorhandler(BadRequest)
def bad_request_handler(error):  # type: ignore  # TODO: fix
    rci = ReqCtxInfoMiddleware.get_last_resort_rci()
    req_id: Optional[str] = rci.request_id if rci is not None else None

    LOGGER.warning(
        'Bad request on %s: %s (folder %s), req_id: %s',
        request.url,
        error.data,
        request.headers.get('X-YaCloud-FolderId'),
        req_id,
    )
    return error.data, 400
