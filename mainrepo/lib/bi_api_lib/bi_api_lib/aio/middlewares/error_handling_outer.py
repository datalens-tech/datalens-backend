from __future__ import annotations

from typing import Optional

from aiohttp import web
import attr

from bi_api_commons.aio.middlewares.error_handling_outer import (
    AIOHTTPErrorHandler,
    ErrorData,
    ErrorLevel,
)
from bi_api_lib.error_handling import (
    BIError,
    PublicAPIErrorSchema,
    RegularAPIErrorSchema,
)


def bi_error_to_error_data(bi_error: BIError, public_mode: bool, reason: Optional[str] = None) -> ErrorData:
    bi_error_schema = PublicAPIErrorSchema() if public_mode else RegularAPIErrorSchema()

    http_response_code = 500 if bi_error.http_code is None else bi_error.http_code
    return ErrorData(
        status_code=http_response_code,
        response_body=bi_error_schema.dump(bi_error),
        level=ErrorLevel.info if http_response_code < 500 else ErrorLevel.error,
    )


# noinspection PyDataclass
@attr.s
class DatasetAPIErrorHandler(AIOHTTPErrorHandler):
    public_mode: bool = attr.ib(kw_only=True)

    def _classify_error(self, err: Exception, request: web.Request) -> ErrorData:
        bi_error: BIError
        reason: Optional[str]

        if isinstance(err, web.HTTPException):
            bi_error = BIError(
                http_code=err.status,
                application_code_stack=(),
                message=err.reason,
                details={},
                debug={},
            )
            reason = err.reason
        else:
            bi_error = BIError.from_exception(err)
            reason = None
        return bi_error_to_error_data(bi_error, public_mode=self.public_mode, reason=reason)
