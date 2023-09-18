from __future__ import annotations

import json
import logging
import time
from typing import Optional

import attr
import flask
from flask import request

from dl_api_commons import (
    log_request_start,
    make_uuid_from_parts,
    request_id_generator,
)
from dl_api_commons.base_models import RequestContextInfo
from dl_api_commons.flask.middlewares.commit_rci_middleware import ReqCtxInfoMiddleware
from dl_api_commons.flask.middlewares.logging_context import RequestLoggingContextControllerMiddleWare
from dl_api_commons.headers import (
    HEADER_DEBUG_MODE_ENABLED,
    HEADER_LOGGING_CONTEXT,
    get_x_dl_context,
)
from dl_api_commons.logging import (
    NON_TRANSITIVE_LOGGING_CTX_KEYS,
    log_request_end_extended,
)
from dl_app_tools.log.context import put_to_context
from dl_constants.api_constants import DLHeadersCommon

LOGGER = logging.getLogger(__name__)


@attr.s
class RequestIDService:
    _request_id_app_prefix: Optional[str] = attr.ib()
    _append_local_req_id: bool = attr.ib(default=True)
    _accept_logging_context: bool = attr.ib(default=False)
    _logging_ctx_header_name: str = attr.ib(default=HEADER_LOGGING_CONTEXT)

    def _before_request(self) -> None:
        flask.g.request_start_time = time.monotonic()

        incoming_req_id = flask.request.headers.get(DLHeadersCommon.REQUEST_ID.value)
        current_req_id: str

        if self._append_local_req_id:
            current_req_id = make_uuid_from_parts(
                current=request_id_generator(self._request_id_app_prefix),
                parent=incoming_req_id,
            )
        else:
            current_req_id = incoming_req_id or request_id_generator(self._request_id_app_prefix)

        req_log_ctx_ctrl = RequestLoggingContextControllerMiddleWare.get_for_request()
        req_log_ctx_ctrl.put_to_context("request_id", current_req_id)
        req_log_ctx_ctrl.put_to_context("parent_request_id", incoming_req_id)

        # Updating logging context with outer values
        logging_ctx_header = request.headers.get(self._logging_ctx_header_name)
        if self._accept_logging_context and logging_ctx_header:
            try:
                logging_ctx_from_header = json.loads(logging_ctx_header)
                for ctx_key in NON_TRANSITIVE_LOGGING_CTX_KEYS:
                    logging_ctx_from_header.pop(ctx_key, None)

                for ctx_key, ctx_val in logging_ctx_from_header.items():
                    put_to_context(ctx_key, ctx_val)

            except Exception:  # noqa
                LOGGER.exception("Can not parse logging context: %s", logging_ctx_header)

        log_request_start(
            logger=LOGGER,
            method=request.method,
            full_path=request.full_path,
            headers=request.headers.items(),
        )

        # Initially setup RCI for request
        ReqCtxInfoMiddleware.replace_temp_rci(
            RequestContextInfo.create(
                request_id=current_req_id,
                x_dl_debug_mode=bool(int(request.headers.get(HEADER_DEBUG_MODE_ENABLED, "0"))),
                endpoint_code=None,
                x_dl_context=get_x_dl_context(request.headers.get(DLHeadersCommon.DL_CONTEXT, "{}")),
                # This props will be filled in commit_rci middleware
                plain_headers=None,
                secret_headers=None,
                # This props will be filled in auth middleware
                tenant=None,
                user_id=None,
                user_name=None,
                auth_data=None,
            )
        )

    def set_up(self, app: flask.Flask) -> None:
        assert RequestLoggingContextControllerMiddleWare.is_initialized_for_app(
            app
        ), "RequestLoggingContextControllerMiddleWare should be setup before request_id"
        app.before_request(self._before_request)
        app.after_request(_set_request_id_response_header)
        app.after_request(_post_log_request_id)


def _post_log_request_id(response: flask.Response) -> flask.Response:
    if response:
        response_timing = None
        request_start_time = getattr(flask.g, "request_start_time", None)
        if request_start_time is not None:
            response_timing = time.monotonic() - request_start_time

        rci = ReqCtxInfoMiddleware.get_last_resort_rci()
        user_name: Optional[str] = None
        user_id: Optional[str] = None
        if rci is not None:
            user_name = rci.user_name
            user_id = rci.user_id

        log_request_end_extended(
            logger=LOGGER,
            request_method=request.method,
            request_path=request.full_path,
            request_headers=dict(request.headers),
            response_status=response.status_code,
            response_headers=dict(response.headers),
            response_timing=response_timing,
            user_id=user_id,
            username=user_name,
        )
        # LOGGER.info('Response content: %r', response.get_json(silent=True))
    return response


def _set_request_id_response_header(response: flask.Response) -> flask.Response:
    if response:
        rci = ReqCtxInfoMiddleware.get_last_resort_rci()
        if rci is not None and rci.request_id is not None:
            response.headers.set("x-request-id", rci.request_id)
    return response
