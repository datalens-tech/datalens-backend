from typing import Optional

from aiohttp import (
    hdrs,
    web,
)
from aiohttp.typedefs import (
    Handler,
    Middleware,
)


ALL_METHODS = ("DELETE", "GET", "OPTIONS", "PATCH", "POST", "PUT")


def cors_middleware(
    allow_origins: tuple[str, ...],
    expose_headers: Optional[tuple[str, ...]] = None,
    allow_headers: Optional[tuple[str, ...]] = None,
    allow_methods: tuple[str, ...] = ALL_METHODS,
    allow_credentials: bool = False,
    max_age: Optional[int] = None,
) -> Middleware:
    @web.middleware
    async def middleware(request: web.Request, handler: Handler) -> web.StreamResponse:
        is_options_request = request.method == "OPTIONS"
        is_preflight_request = is_options_request and hdrs.ACCESS_CONTROL_REQUEST_METHOD in request.headers
        origin = request.headers.get(hdrs.ORIGIN)
        allow_all_origins = "*" in allow_origins

        if is_preflight_request:
            response = web.StreamResponse()
        else:
            response = await handler(request)

        if not origin:
            return response

        if not (origin in allow_origins or allow_all_origins):
            return response

        if allow_all_origins and not allow_credentials:
            cors_origin = "*"
        else:
            cors_origin = origin
        response.headers[hdrs.ACCESS_CONTROL_ALLOW_ORIGIN] = cors_origin

        if allow_credentials:
            response.headers[hdrs.ACCESS_CONTROL_ALLOW_CREDENTIALS] = "true"

        if expose_headers:
            response.headers[hdrs.ACCESS_CONTROL_EXPOSE_HEADERS] = ",".join(expose_headers)

        if is_options_request:
            if allow_headers:
                response.headers[hdrs.ACCESS_CONTROL_ALLOW_HEADERS] = ",".join(allow_headers)
            if allow_methods:
                response.headers[hdrs.ACCESS_CONTROL_ALLOW_METHODS] = ",".join(allow_methods)
            if max_age is not None:
                response.headers[hdrs.ACCESS_CONTROL_MAX_AGE] = str(max_age)

        if is_preflight_request:
            # skip other middlewares
            raise web.HTTPOk(text="", headers=response.headers)

        return response

    return middleware
