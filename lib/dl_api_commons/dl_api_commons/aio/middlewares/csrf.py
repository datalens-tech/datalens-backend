import hmac
import logging
import time
from typing import (
    ClassVar,
    Optional,
)

from aiohttp import web
from aiohttp.typedefs import Handler
import attr

from dl_api_commons.aiohttp import aiohttp_wrappers


LOGGER = logging.getLogger(__name__)


def generate_csrf_token(user_id: str, timestamp: int, csrf_secret: str) -> str:
    msg = bytes("{}:{}".format(user_id, timestamp), encoding="utf-8")
    secret = bytes(csrf_secret, encoding="utf-8")
    h = hmac.new(key=secret, msg=msg, digestmod="sha1")
    return h.hexdigest()


@attr.s(frozen=True)
class CSRFMiddleware:
    USER_ID_COOKIES: ClassVar[tuple[str, ...]] = ()

    csrf_header_name: str = attr.ib()
    csrf_time_limit: int = attr.ib()
    csrf_secrets: tuple[str, ...] = attr.ib()
    csrf_methods: tuple[str, ...] = attr.ib(default=("POST", "PUT", "DELETE"))

    def validate_csrf_token(self, token_header_value: Optional[str], user_token: str) -> bool:
        if not token_header_value:
            return False

        try:
            token, timestamp_str = token_header_value.split(":")
            timestamp = int(timestamp_str)
        except ValueError:
            return False

        if not token or timestamp <= 0:
            return False

        ts_now = int(time.time())
        if ts_now - timestamp > self.csrf_time_limit:
            return False

        for csrf_secret in self.csrf_secrets:
            if hmac.compare_digest(generate_csrf_token(user_token, timestamp, csrf_secret), token):
                return True

        return False

    @web.middleware
    @aiohttp_wrappers.DLRequestBase.use_dl_request_on_method
    async def middleware(self, dl_request: aiohttp_wrappers.DLRequestBase, handler: Handler) -> web.StreamResponse:
        async def _continue_request() -> web.StreamResponse:
            return await handler(dl_request.request)

        if aiohttp_wrappers.RequiredResourceCommon.SKIP_CSRF in dl_request.required_resources:
            LOGGER.info("CSRF check was skipped due to SKIP_CSRF flag in target view")
            return await _continue_request()

        if dl_request.request.method not in self.csrf_methods or not dl_request.request.cookies:
            return await _continue_request()

        rci = dl_request.last_resort_rci
        user_tokens = []
        if rci is not None and rci.user_id:
            user_tokens.append(rci.user_id)
        for user_id_cookie in self.USER_ID_COOKIES:
            user_token = dl_request.request.cookies.get(user_id_cookie)
            if user_token is not None:
                user_tokens.append(user_token)

        if not user_tokens:
            return await _continue_request()

        csrf_token = dl_request.request.headers.get(self.csrf_header_name)

        LOGGER.info("Checking CSRF token for user tokens: %s", user_tokens)

        token_is_valid = None
        for user_token in user_tokens:
            token_is_valid = self.validate_csrf_token(csrf_token, user_token)
            if token_is_valid:
                LOGGER.info("CSRF token is valid for user token %s", user_token)
                break

        if not token_is_valid:
            LOGGER.info("CSRF validation failed.")
            return web.Response(body="CSRF validation failed", content_type="text/html", status=400)

        return await handler(dl_request.request)
