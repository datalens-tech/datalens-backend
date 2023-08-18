from __future__ import annotations

import json
import logging
from http import HTTPStatus

from aiohttp import web
from marshmallow import exceptions as ma_exc

from bi_api_commons.aio.middlewares.error_handling_outer import AIOHTTPErrorHandler, ErrorData, ErrorLevel
from .. import exc


LOGGER = logging.getLogger(__name__)


STATUS_CODES = {
    exc.NotFoundError: HTTPStatus.NOT_FOUND,
}


class AlertsErrorHandler(AIOHTTPErrorHandler):
    def _classify_error(self, err: Exception, request: web.Request) -> ErrorData:
        if isinstance(err, ma_exc.ValidationError):
            return ErrorData(
                status_code=HTTPStatus.BAD_REQUEST,
                response_body=dict(message=str(err), code='ERR.ALERT.SCHEMA_VALIDATION'),
                level=ErrorLevel.info,
            )
        elif isinstance(err, json.decoder.JSONDecodeError):
            return ErrorData(
                status_code=HTTPStatus.BAD_REQUEST,
                response_body=dict(message=str(err), code='ERR.ALERT.BAD_JSON'),
                level=ErrorLevel.info,
            )
        elif isinstance(err, exc.DLAlertBaseError):
            status_code = STATUS_CODES[err.__class__]  # type: ignore  # TODO: fix
            body = dict(
                message=err.message,
                code='.'.join(['ERR'] + err.err_code),
                debug=err.debug_info,
                details=err.details,
            )
            LOGGER.info('Raising DLAlertBaseError, status_code: %s, data: %s', status_code, body)
            return ErrorData(
                status_code=status_code,
                response_body=body,
                level=ErrorLevel.info,
            )
        elif isinstance(err, web.HTTPException):
            return ErrorData(
                status_code=err.status,
                http_reason=err.reason,
                response_body=dict(message=err.reason),
                level=ErrorLevel.info,
            )
        else:
            LOGGER.exception('Unhandled exception')
            return ErrorData(
                status_code=500,
                http_reason="Internal server error",
                response_body=dict(message="Internal server error"),
                level=ErrorLevel.error,
            )
