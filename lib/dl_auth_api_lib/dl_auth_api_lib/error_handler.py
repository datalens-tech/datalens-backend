from http import HTTPStatus

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
        else:
            return DEFAULT_INTERNAL_SERVER_ERROR_DATA
