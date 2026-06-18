import attrs
import httpx


class BaseHttpxClientError(Exception): ...


@attrs.define(kw_only=True, auto_attribs=True, frozen=True)
class HttpStatusHttpxClientError(BaseHttpxClientError):
    request: httpx.Request
    response: httpx.Response


@attrs.define(kw_only=True, auto_attribs=True, frozen=True)
class RequestHttpxClientError(BaseHttpxClientError):
    original_exception: Exception


@attrs.define(kw_only=True, auto_attribs=True, frozen=True)
class NoRetriesHttpxClientError(BaseHttpxClientError): ...
