import typing

from aiohttp import web
from aiohttp.typedefs import (
    Handler,
    Middleware,
)

from dl_api_commons.crypto import get_hmac_hex_digest


def body_signature_validation_middleware(hmac_keys: typing.Sequence[bytes], header: str) -> Middleware:
    if not hmac_keys:
        raise Exception("body_signature_validation_middleware: no hmac_keys passed")

    if any(not key for key in hmac_keys):
        raise Exception("body_signature_validation_middleware: empty hmac_key passed")

    @web.middleware
    async def actual_middleware(request: web.Request, handler: Handler) -> web.StreamResponse:
        if request.method in ("HEAD", "OPTIONS", "GET"):
            return await handler(request)

        body_bytes = await request.read()
        signature_str_from_header = request.headers.get(header)
        for hmac_key in hmac_keys:
            expected_signature = get_hmac_hex_digest(body_bytes, secret_key=hmac_key)

            if expected_signature == signature_str_from_header:
                return await handler(request)

        raise web.HTTPForbidden(reason="Invalid signature")

    return actual_middleware
