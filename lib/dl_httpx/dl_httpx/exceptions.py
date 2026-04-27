import attrs
import httpx


class BaseHttpxClientException(Exception):
    ...


@attrs.define(kw_only=True, auto_attribs=True, frozen=True)
class HttpStatusHttpxClientException(BaseHttpxClientException):
    request: httpx.Request
    response: httpx.Response


@attrs.define(kw_only=True, auto_attribs=True, frozen=True)
class RequestHttpxClientException(BaseHttpxClientException):
    original_exception: Exception


@attrs.define(kw_only=True, auto_attribs=True, frozen=True)
class NoRetriesHttpxClientException(BaseHttpxClientException):
    ...
