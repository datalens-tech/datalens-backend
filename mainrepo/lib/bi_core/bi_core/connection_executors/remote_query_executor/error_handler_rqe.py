from __future__ import annotations

import logging

from aiohttp import web

from bi_api_commons.aio.middlewares.error_handling_outer import AIOHTTPErrorHandler, ErrorData, ErrorLevel

from bi_core.connection_executors.qe_serializer import ActionSerializer


LOGGER = logging.getLogger(__name__)


class RQEErrorHandler(AIOHTTPErrorHandler):
    def _classify_error(self, err: Exception, request: web.Request) -> ErrorData:
        if isinstance(err, web.HTTPException):
            return ErrorData(
                err.status,
                http_reason=err.reason,
                response_body=ActionSerializer().serialize_exc(err),
                level=ErrorLevel.info,
            )
        else:
            LOGGER.exception('RQE-async handler error: %r', err)
            return ErrorData(
                500,
                http_reason='Internal server error',
                response_body=ActionSerializer().serialize_exc(err),
                level=ErrorLevel.error,
            )
