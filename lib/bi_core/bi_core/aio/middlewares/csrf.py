from typing import Optional

import hmac
import logging
import time

from aiohttp import web
from aiohttp.typedefs import Handler

from bi_api_commons.aiohttp import aiohttp_wrappers
from bi_api_commons.aio.typing import AIOHTTPMiddleware


LOGGER = logging.getLogger(__name__)


def generate_csrf_token(yandexuid: str, timestamp: int, csrf_secret: str) -> str:
    msg = bytes('{}:{}'.format(yandexuid, timestamp), encoding='utf-8')
    secret = bytes(csrf_secret, encoding='utf-8')
    h = hmac.new(key=secret, msg=msg, digestmod='sha1')
    return h.hexdigest()


def csrf_middleware(
    csrf_header_name: str,
    csrf_time_limit: int,
    csrf_secret: str,
    csrf_methods: tuple[str, ...] = ('POST', 'PUT', 'DELETE'),
) -> AIOHTTPMiddleware:
    def validate_csrf_token(token_header_value: Optional[str], user_token: str) -> bool:
        if not token_header_value:
            return False

        try:
            token, timestamp_str = token_header_value.split(':')
            timestamp = int(timestamp_str)
        except ValueError:
            return False

        if not token or timestamp <= 0:
            return False

        ts_now = int(time.time())
        if ts_now - timestamp > csrf_time_limit:
            return False

        if not hmac.compare_digest(generate_csrf_token(user_token, timestamp, csrf_secret), token):
            return False

        return True

    @web.middleware
    @aiohttp_wrappers.DLRequestBase.use_dl_request
    async def actual_csrf_middleware(
        dl_request: aiohttp_wrappers.DLRequestBase,
        handler: Handler
    ) -> web.StreamResponse:
        async def _continue_request() -> web.StreamResponse:
            return await handler(dl_request.request)

        if aiohttp_wrappers.RequiredResourceCommon.SKIP_CSRF in dl_request.required_resources:
            LOGGER.info("CSRF check was skipped due to SKIP_CSRF flag in target view")
            return await _continue_request()

        if dl_request.request.method not in csrf_methods or not dl_request.request.cookies:
            return await _continue_request()

        # Frontend in Cloud installation is migrating from "yandexuid" cookie usage
        # for CSRF token generation to IAM user_id.
        # Currently both options possible:
        #   - yandexuid - for Blackbox authenticated users
        #   - user_id - for federation users (authenticated in IAM SessionService).
        #
        # https://st.yandex-team.ru/BI-1757#5f569d3e035fc353df8eec82

        rci = dl_request.last_resort_rci
        user_tokens = []
        if rci is not None and rci.user_id:
            user_tokens.append(rci.user_id)
        yandexuid = dl_request.request.cookies.get('yandexuid')
        if yandexuid is not None:
            user_tokens.append(yandexuid)

        if not user_tokens:
            return await _continue_request()

        csrf_token = dl_request.request.headers.get(csrf_header_name)

        LOGGER.info('Checking CSRF token for user tokens: %s', user_tokens)

        token_is_valid = None
        for user_token in user_tokens:
            token_is_valid = validate_csrf_token(csrf_token, user_token)
            if token_is_valid:
                LOGGER.info('CSRF token is valid for user token %s', user_token)
                break

        if not token_is_valid:
            LOGGER.info('CSRF validation failed.')
            return web.Response(body='CSRF validation failed', content_type='text/html', status=400)

        return await handler(dl_request.request)

    return actual_csrf_middleware
