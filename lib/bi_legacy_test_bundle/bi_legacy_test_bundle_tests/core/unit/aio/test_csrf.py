from __future__ import annotations

from typing import Dict, Optional, Callable, Awaitable, ClassVar

import pytest
import time

from aiohttp import web
from aiohttp.test_utils import TestClient

from bi_api_commons.aio.middlewares.request_bootstrap import RequestBootstrap
from bi_api_commons.aio.middlewares.request_id import RequestId
from bi_api_commons.aiohttp.aiohttp_wrappers import DLRequestView, RequiredResourceCommon, RequiredResource
from bi_core.aio.middlewares.auth_trust_middleware import auth_trust_middleware
from bi_core.aio.middlewares.csrf import CSRFMiddleware, generate_csrf_token


_SAMPLE_YANDEXUID = _SAMPLE_USER_ID = '123'
_SAMPLE_TIMESTAMP = 1
_VALID_CSRF_SECRET = 'valid_secret'
_INVALID_CSRF_SECRET = 'invalid_secret'
_CSRF_TIME_LIMIT = 3600 * 12

_AppFactory = Callable[[bool], Awaitable[TestClient]]


def ts_now():
    return int(time.time())


class CSRFMiddlewareYa(CSRFMiddleware):
    USER_ID_COOKIES = ('yandexuid',)


@pytest.fixture(scope='function')
async def csrf_app_factory(aiohttp_client) -> _AppFactory:
    async def f(authorized: Optional[bool]) -> TestClient:
        async def non_csrf_handler(request: web.Request):
            return web.json_response(dict(ok='ok'), status=200)

        async def csrf_handler(request: web.Request):
            return web.json_response(dict(ok='ok'), status=200)

        class SkipCSRFView(DLRequestView):
            REQUIRED_RESOURCES: ClassVar[frozenset[RequiredResource]] = frozenset({RequiredResourceCommon.SKIP_CSRF})

            async def put(self) -> web.StreamResponse:
                return web.json_response(dict(ok='ok'), status=200)

        req_id_service = RequestId(
            append_own_req_id=True,
            app_prefix="f",
        )

        app = web.Application(middlewares=[
            RequestBootstrap(
                req_id_service=req_id_service,
            ).middleware,
            auth_trust_middleware(fake_user_id=_SAMPLE_USER_ID if authorized else None),
            CSRFMiddlewareYa(
                csrf_header_name='x-csrf-token',
                csrf_time_limit=_CSRF_TIME_LIMIT,
                csrf_secret=_VALID_CSRF_SECRET,
                csrf_methods=('POST', 'PUT', 'DELETE'),
            ).middleware,
        ])

        app.router.add_get('/', non_csrf_handler)
        app.router.add_post('/', csrf_handler)
        app.router.add_put('/', SkipCSRFView)

        return await aiohttp_client(app)

    return f


@pytest.mark.asyncio
@pytest.mark.parametrize('case_name, method, authorized, headers, cookies', [
    (
        'OK_just_yandexuid',
        'POST',
        False,
        {'x-csrf-token': '{}:{}'.format(
            generate_csrf_token(_SAMPLE_YANDEXUID, ts_now(), _VALID_CSRF_SECRET),
            ts_now()
        )},
        {'yandexuid': _SAMPLE_YANDEXUID},
    ),
    (
        'OK_just_user_id',
        'POST',
        True,
        {'x-csrf-token': '{}:{}'.format(
            generate_csrf_token(_SAMPLE_USER_ID, ts_now(), _VALID_CSRF_SECRET),
            ts_now()
        )},
        {'does_not': 'matter'},
    ),
    (
        'OK_yandexuid_and_user_id',
        'POST',
        True,
        {'x-csrf-token': '{}:{}'.format(
            generate_csrf_token(_SAMPLE_YANDEXUID, ts_now(), _VALID_CSRF_SECRET),
            ts_now()
        )},
        {'yandexuid': _SAMPLE_YANDEXUID},
    ),
    (
        'SKIP_non_csrf_method',
        'GET',
        False,
        {'x-csrf-token': '{}:{}'.format(
            generate_csrf_token(_SAMPLE_YANDEXUID, ts_now(), _VALID_CSRF_SECRET),
            ts_now()
        )},
        {'yandexuid': _SAMPLE_YANDEXUID},
    ),
    (
        'SKIP_no_cookies',
        'POST',
        True,
        {'x-csrf-token': '{}:{}'.format(
            generate_csrf_token(_SAMPLE_YANDEXUID, ts_now(), _VALID_CSRF_SECRET),
            ts_now()
        )},
        None,
    ),
    (
        'SKIP_no_user_tokens',
        'POST',
        False,
        {'x-csrf-token': '{}:{}'.format(
            generate_csrf_token(_SAMPLE_YANDEXUID, ts_now(), _VALID_CSRF_SECRET),
            ts_now()
        )},
        {'does_not': 'matter'},
    ),
    (
        'SKIP_view_marker',
        'PUT',
        False,
        {'x-csrf-token': 'some_token'},
        {'yandexuid': _SAMPLE_YANDEXUID},
    ),
])
async def test_csrf_ok(
    case_name: str,
    method: str,
    authorized: bool,
    headers: Dict[str, str],
    cookies: Dict[str, str],
    csrf_app_factory: _AppFactory,
):
    client = await csrf_app_factory(authorized)
    resp = await client.request(
        method=method,
        path='/',
        headers=headers,
        cookies=cookies,
    )
    assert resp.status == 200
    resp_json = await resp.json()
    assert resp_json == dict(ok='ok')


@pytest.mark.asyncio
@pytest.mark.parametrize('case_name, method, authorized, headers, cookies', [
    (
        'INVALID_no_csrf_token_provided',
        'POST',
        False,
        {},
        {'yandexuid': _SAMPLE_YANDEXUID},
    ),
    (
        'INVALID_malformed_csrf_token_1',
        'POST',
        False,
        {'x-csrf-token': 'asdf:1234:5678'},
        {'yandexuid': _SAMPLE_YANDEXUID},
    ),
    (
        'INVALID_malformed_csrf_token_2',
        'POST',
        False,
        {'x-csrf-token': 'asdf:qwer'},
        {'yandexuid': _SAMPLE_YANDEXUID},
    ),
    (
        'INVALID_empty_token',
        'POST',
        False,
        {'x-csrf-token': ':1234'},
        {'yandexuid': _SAMPLE_YANDEXUID},
    ),
    (
        'INVALID_token_expired',
        'POST',
        False,
        {'x-csrf-token': '{}:{}'.format(
            generate_csrf_token(_SAMPLE_YANDEXUID, ts_now() - 2*_CSRF_TIME_LIMIT, _VALID_CSRF_SECRET),
            ts_now() - 2*_CSRF_TIME_LIMIT
        )},
        {'yandexuid': _SAMPLE_YANDEXUID},
    ),
    (
        'INVALID_bad_secret',
        'POST',
        False,
        {'x-csrf-token': '{}:{}'.format(
            generate_csrf_token(_SAMPLE_USER_ID, ts_now(), _INVALID_CSRF_SECRET),
            ts_now()
        )},
        {'yandexuid': _SAMPLE_USER_ID},
    )
])
async def test_csrf_invalid(
    case_name: str,
    method: str,
    authorized: bool,
    headers: Dict[str, str],
    cookies: Dict[str, str],
    csrf_app_factory: _AppFactory,
):
    validation_failed_text = 'CSRF validation failed'
    validation_failed_status = 400

    client = await csrf_app_factory(authorized)
    resp = await client.request(
        method=method,
        path='/',
        headers=headers,
        cookies=cookies,
    )
    assert resp.status == validation_failed_status
    resp_text = await resp.text()
    assert resp_text == validation_failed_text
