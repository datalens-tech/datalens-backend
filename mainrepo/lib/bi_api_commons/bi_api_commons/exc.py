from typing import (
    Any,
    Generic,
    Optional,
    TypeVar,
)

import attr

from bi_constants.exc import DLBaseException

_EXC_DATA_TV = TypeVar("_EXC_DATA_TV")


def format_response_body(obj: Any) -> str:
    str_limit = 500
    tail_placeholder = "...truncated..."

    if isinstance(obj, str):
        if len(obj) > str_limit:
            return repr(obj[: str_limit - len(tail_placeholder)] + tail_placeholder)
        else:
            return repr(obj)
    else:
        orig_repr = repr(obj)
        return format_response_body(orig_repr)


class ExceptionWithData(Generic[_EXC_DATA_TV], Exception):
    _data: _EXC_DATA_TV

    def __init__(self, data: _EXC_DATA_TV):
        super().__init__(data)
        self._data = data

    @property
    def data(self) -> _EXC_DATA_TV:
        return self._data


@attr.s()
class APIResponseData:
    operation: str = attr.ib()
    status_code: Optional[int] = attr.ib()
    response_body: Optional[Any] = attr.ib(repr=format_response_body)
    response_body_validation_errors: Optional[dict[str, Any]] = attr.ib(default=None)


class InternalAPIBadResponseErr(ExceptionWithData[APIResponseData]):
    CODE = "INTERNAL_API_BAD_RESPONSE"


class AccessDeniedErr(InternalAPIBadResponseErr):
    CODE = "ACCESS_DENIED"


class NotFoundErr(InternalAPIBadResponseErr):
    CODE = "NOT_FOUND"


class MalformedAPIResponseErr(InternalAPIBadResponseErr):
    CODE = "MALFORMED_API_RESPONSE"


class InvalidHeaderException(Exception):
    schema_validation_messages: Optional[dict]
    header_name: str

    def __init__(self, *args: Any, header_name: str, schema_validation_messages: Optional[dict] = None):
        self.header_name = header_name
        self.schema_validation_messages = schema_validation_messages
        super().__init__(*args)


class FlaskRCINotSet(Exception):
    pass


class RequestTimeoutError(DLBaseException):
    err_code = DLBaseException.err_code + ["REQUEST_TIMEOUT"]
    default_message = "Backend app request timeout exceeded"
