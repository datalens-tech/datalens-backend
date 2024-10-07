from http import HTTPStatus
from typing import Type

from aiohttp import (
    client_exceptions,
    web,
)
from marshmallow import ValidationError as MValidationError

from dl_api_commons.aio.middlewares.error_handling_outer import (
    DEFAULT_INTERNAL_SERVER_ERROR_DATA,
    AIOHTTPErrorHandler,
    ErrorData,
    ErrorLevel,
)
from dl_auth_api_lib import exc


STATUS_CODES: dict[Type[exc.DLAuthAPIBaseError], HTTPStatus] = {
    exc.UnexpectedResponseError: HTTPStatus.BAD_REQUEST,
}


class OAuthApiErrorHandler(AIOHTTPErrorHandler):
    def _classify_error(self, err: Exception, request: web.Request) -> ErrorData:
        if isinstance(err, web.HTTPException):
            return ErrorData(
                status_code=err.status,
                http_reason=err.reason,
                response_body=dict(message=err.reason),
                level=ErrorLevel.info,
            )
        elif isinstance(err, client_exceptions.ClientResponseError):
            return ErrorData(
                status_code=HTTPStatus.BAD_REQUEST,
                response_body=dict(message=str(err)),
                level=ErrorLevel.info,
            )
        elif isinstance(err, MValidationError):
            return ErrorData(
                status_code=HTTPStatus.BAD_REQUEST,
                response_body=dict(message=str(err)),
                level=ErrorLevel.info,
            )
        elif isinstance(err, exc.DLAuthAPIBaseError):
            status_code = STATUS_CODES.get(err.__class__, HTTPStatus.INTERNAL_SERVER_ERROR)
            body = dict(
                message=err.message,
                code=exc.make_err_code(err),
                debug=err.debug_info,
                details=err.details,
            )
            return ErrorData(
                status_code=status_code,
                response_body=body,
                level=ErrorLevel.info,
            )
        else:
            return DEFAULT_INTERNAL_SERVER_ERROR_DATA
