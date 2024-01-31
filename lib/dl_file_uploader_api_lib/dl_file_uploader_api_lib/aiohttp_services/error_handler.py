from http import HTTPStatus

import aiogoogle
from aiohttp import web
from marshmallow import ValidationError as MValidationError

from dl_api_commons.aio.middlewares.error_handling_outer import (
    AIOHTTPErrorHandler,
    ErrorData,
    ErrorLevel,
)
from dl_file_uploader_lib import exc
from dl_file_uploader_lib.gsheets_client import google_api_error_to_file_uploader_exception
from dl_file_uploader_lib.redis_model.base import (
    RedisModelAccessDenied,
    RedisModelNotFound,
)
from dl_file_uploader_lib.redis_model.models import models


STATUS_CODES = {
    exc.CannotUpdateDataError: HTTPStatus.BAD_REQUEST,
    exc.DocumentNotFound: HTTPStatus.BAD_REQUEST,
    exc.DownloadFailed: HTTPStatus.INTERNAL_SERVER_ERROR,
    exc.EmptyDocument: HTTPStatus.BAD_REQUEST,
    exc.FileLimitError: HTTPStatus.REQUEST_ENTITY_TOO_LARGE,
    exc.InvalidFieldCast: HTTPStatus.BAD_REQUEST,
    exc.InvalidLink: HTTPStatus.BAD_REQUEST,
    exc.InvalidSource: HTTPStatus.BAD_REQUEST,
    exc.ParseFailed: HTTPStatus.INTERNAL_SERVER_ERROR,
    exc.PermissionDenied: HTTPStatus.BAD_REQUEST,
    exc.RemoteServerError: HTTPStatus.BAD_REQUEST,
    exc.UnsupportedDocument: HTTPStatus.BAD_REQUEST,
}


class FileUploaderErrorHandler(AIOHTTPErrorHandler):
    def _classify_error(self, err: Exception, request: web.Request) -> ErrorData:
        if isinstance(err, aiogoogle.HTTPError):
            err = google_api_error_to_file_uploader_exception(err)

        if isinstance(err, web.HTTPException):
            return ErrorData(
                status_code=err.status,
                http_reason=err.reason,
                response_body=dict(message=err.reason),
                level=ErrorLevel.info,
            )
        elif isinstance(err, MValidationError):
            return ErrorData(
                status_code=HTTPStatus.BAD_REQUEST,
                response_body=dict(message=str(err)),
                level=ErrorLevel.info,
            )
        elif isinstance(err, RedisModelAccessDenied):
            return ErrorData(HTTPStatus.FORBIDDEN, response_body={}, level=ErrorLevel.info)
        elif isinstance(err, RedisModelNotFound):
            return ErrorData(HTTPStatus.NOT_FOUND, response_body={}, level=ErrorLevel.info)
        elif isinstance(err, (models.EmptySourcesError, models.SourceNotFoundError)):
            return ErrorData(HTTPStatus.NOT_FOUND, response_body={}, level=ErrorLevel.info)
        elif isinstance(err, exc.DLFileUploaderBaseError):
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
            return ErrorData(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                http_reason="Internal server error",
                response_body=dict(message="Internal server error"),
                level=ErrorLevel.error,
            )
