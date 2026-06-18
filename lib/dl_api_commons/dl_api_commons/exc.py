from typing import Any

import attr

from dl_constants.exc import DLBaseError


def format_response_body(obj: Any) -> str:
    str_limit = 500
    tail_placeholder = "...truncated..."

    if isinstance(obj, str):
        if len(obj) > str_limit:
            return repr(obj[: str_limit - len(tail_placeholder)] + tail_placeholder)
        return repr(obj)
    orig_repr = repr(obj)
    return format_response_body(orig_repr)


class ExceptionWithDataError[EXC_DATA_TV](Exception):
    _data: EXC_DATA_TV

    def __init__(self, data: EXC_DATA_TV) -> None:
        super().__init__(data)
        self._data = data

    @property
    def data(self) -> EXC_DATA_TV:
        return self._data


@attr.s()
class APIResponseData:
    operation: str = attr.ib()
    status_code: int | None = attr.ib()
    response_body: Any | None = attr.ib(repr=format_response_body)
    response_body_validation_errors: dict[str, Any] | None = attr.ib(default=None)


class InternalAPIBadResponseErr(ExceptionWithDataError[APIResponseData]):
    CODE = "INTERNAL_API_BAD_RESPONSE"


class AccessDeniedErr(InternalAPIBadResponseErr):
    CODE = "ACCESS_DENIED"


class NotFoundErr(InternalAPIBadResponseErr):
    CODE = "NOT_FOUND"


class MalformedAPIResponseErr(InternalAPIBadResponseErr):
    CODE = "MALFORMED_API_RESPONSE"


class InvalidHeaderError(Exception):
    schema_validation_messages: dict | None
    header_name: str

    def __init__(self, *args: Any, header_name: str, schema_validation_messages: dict | None = None) -> None:
        self.header_name = header_name
        self.schema_validation_messages = schema_validation_messages
        super().__init__(*args)


class FlaskRCINotSetError(Exception):
    pass


class RequestTimeoutError(DLBaseError):
    err_code = (*DLBaseError.err_code, "REQUEST_TIMEOUT")
    default_message = "Backend app request timeout exceeded"


class FailedDependencyError(DLBaseError):
    err_code = (*DLBaseError.err_code, "FAILED_DEPENDENCY")
    default_message = "Failed dependency"
