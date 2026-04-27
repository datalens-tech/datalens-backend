from typing import Any

import httpx

import dl_httpx


def make_http_status_exception(
    status_code: int,
    json_body: Any = None,
    text_body: str | None = None,
) -> dl_httpx.HttpStatusHttpxClientException:
    request = httpx.Request("GET", "https://example.com/api")
    if json_body is not None:
        response = httpx.Response(status_code=status_code, json=json_body, request=request)
    elif text_body is not None:
        response = httpx.Response(status_code=status_code, text=text_body, request=request)
    else:
        response = httpx.Response(status_code=status_code, request=request)
    return dl_httpx.HttpStatusHttpxClientException(request=request, response=response)
