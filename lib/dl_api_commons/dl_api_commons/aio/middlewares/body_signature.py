from aiohttp import web
from aiohttp.typedefs import Handler

from dl_api_commons.aio.typing import AIOHTTPMiddleware
from dl_api_commons.crypto import get_hmac_hex_digest


def body_signature_validation_middleware(hmac_key: bytes, header: str) -> AIOHTTPMiddleware:
    @web.middleware
    async def actual_middleware(request: web.Request, handler: Handler) -> web.StreamResponse:
        if not hmac_key:  # do not consider an empty hmac key as valid.
            raise Exception("body_signature_validation_middleware: no hmac_key.")

        if request.method in ("HEAD", "OPTIONS", "GET"):
            return await handler(request)

        body_bytes = await request.read()
        expected_signature = get_hmac_hex_digest(body_bytes, secret_key=hmac_key)
        signature_str_from_header = request.headers.get(header)

        if expected_signature != signature_str_from_header:
            raise web.HTTPForbidden(reason="Invalid signature")

        return await handler(request)

    return actual_middleware
