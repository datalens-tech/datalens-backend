from __future__ import annotations

from typing import Type

from dl_api_commons.aio.middlewares.error_handling_outer import ErrorLevel
from dl_constants.exc import (
    GLOBAL_ERR_PREFIX,
    DLBaseException,
)


def make_err_code(exc: Type[DLAuthAPIBaseError] | DLAuthAPIBaseError) -> str:
    return ".".join([GLOBAL_ERR_PREFIX] + exc.err_code)


class DLAuthAPIBaseError(DLBaseException):
    err_code = DLBaseException.err_code + ["AUTH_API"]
    default_level = ErrorLevel.error


class UnexpectedResponseError(DLAuthAPIBaseError):
    err_code = DLAuthAPIBaseError.err_code + ["UNEXPECTED_RESPONSE"]
    default_message = "Got an unexpected response from an external API"
