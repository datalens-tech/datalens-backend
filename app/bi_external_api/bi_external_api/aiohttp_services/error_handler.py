from aiohttp import web

from bi_api_commons.aio.middlewares.error_handling_outer import AIOHTTPErrorHandler, ErrorData, ErrorLevel


class ExternalAPIErrorHandler(AIOHTTPErrorHandler):
    def _classify_error(self, err: Exception, request: web.Request) -> ErrorData:
        if isinstance(err, web.HTTPException):
            return ErrorData(
                err.status,
                http_reason=err.reason,
                response_body=dict(message=err.reason),
                level=ErrorLevel.info,
            )
        else:
            return ErrorData(
                500,
                http_reason="Internal server error",
                response_body=dict(message="Internal server error"),
                level=ErrorLevel.error,
            )
