from __future__ import annotations

import time
from typing import (
    Awaitable,
    Callable,
    ClassVar,
    Optional,
)

from aiohttp import web
from aiohttp.test_utils import TestClient
import pytest
import pytest_asyncio

from dl_api_commons.aio.middlewares.auth_trust_middleware import auth_trust_middleware
from dl_api_commons.aio.middlewares.csrf import (
    CSRFMiddleware,
    generate_csrf_token,
)
from dl_api_commons.aio.middlewares.request_bootstrap import RequestBootstrap
from dl_api_commons.aio.middlewares.request_id import RequestId
from dl_api_commons.aiohttp.aiohttp_wrappers import (
    DLRequestView,
    RequiredResource,
    RequiredResourceCommon,
)


_SAMPLE_USER_ID = "123"
_SAMPLE_TIMESTAMP = 1
_VALID_CSRF_SECRET = "valid_secret"
_ANOTHER_VALID_CSRF_SECRET = "another_valid_secret"
_INVALID_CSRF_SECRET = "invalid_secret"
_CSRF_TIME_LIMIT = 3600 * 12

_AppFactory = Callable[[bool, tuple[str, ...]], Awaitable[TestClient]]


def ts_now() -> int:
    return int(time.time())


class TestingCSRFMiddleware(CSRFMiddleware):
    USER_ID_COOKIES = ("user_id_cookie",)


@pytest_asyncio.fixture(scope="function")
async def csrf_app_factory(aiohttp_client: TestClient) -> _AppFactory:
    async def f(authorized: Optional[bool], secrets: tuple[str, ...]) -> TestClient:
        async def non_csrf_handler(request: web.Request) -> web.Response:
            return web.json_response(dict(ok="ok"), status=200)

        async def csrf_handler(request: web.Request) -> web.Response:
            return web.json_response(dict(ok="ok"), status=200)

        class SkipCSRFView(DLRequestView):
            REQUIRED_RESOURCES: ClassVar[frozenset[RequiredResource]] = frozenset({RequiredResourceCommon.SKIP_CSRF})

            async def put(self) -> web.Response:
                return web.json_response(dict(ok="ok"), status=200)

        req_id_service = RequestId(
            append_own_req_id=True,
            app_prefix="f",
        )

        app = web.Application(
            middlewares=[
                RequestBootstrap(
                    req_id_service=req_id_service,
                ).middleware,
                auth_trust_middleware(fake_user_id=_SAMPLE_USER_ID if authorized else None),
                TestingCSRFMiddleware(
                    csrf_header_name="x-csrf-token",
                    csrf_time_limit=_CSRF_TIME_LIMIT,
                    csrf_secrets=secrets,
                    csrf_methods=("POST", "PUT", "DELETE"),
                ).middleware,
            ]
        )

        app.router.add_get("/", non_csrf_handler)
        app.router.add_post("/", csrf_handler)
        app.router.add_put("/", SkipCSRFView)

        return await aiohttp_client(app)  # type: ignore[operator]

    return f


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case_name, method, authorized, headers, cookies, secrets",
    [
        (
            "OK_just_cookie",
            "POST",
            False,
            {
                "x-csrf-token": "{}:{}".format(
                    generate_csrf_token(_SAMPLE_USER_ID, ts_now(), _VALID_CSRF_SECRET), ts_now()
                )
            },
            {"user_id_cookie": _SAMPLE_USER_ID},
            (_VALID_CSRF_SECRET,),
        ),
        (
            "OK_just_user_id",
            "POST",
            True,
            {
                "x-csrf-token": "{}:{}".format(
                    generate_csrf_token(_SAMPLE_USER_ID, ts_now(), _VALID_CSRF_SECRET), ts_now()
                )
            },
            {"does_not": "matter"},
            (_VALID_CSRF_SECRET,),
        ),
        (
            "OK_cookie_and_user_id",
            "POST",
            True,
            {
                "x-csrf-token": "{}:{}".format(
                    generate_csrf_token(_SAMPLE_USER_ID, ts_now(), _VALID_CSRF_SECRET), ts_now()
                )
            },
            {"user_id_cookie": _SAMPLE_USER_ID},
            (_VALID_CSRF_SECRET,),
        ),
        (
            "SKIP_non_csrf_method",
            "GET",
            False,
            {
                "x-csrf-token": "{}:{}".format(
                    generate_csrf_token(_SAMPLE_USER_ID, ts_now(), _VALID_CSRF_SECRET), ts_now()
                )
            },
            {"user_id_cookie": _SAMPLE_USER_ID},
            (_VALID_CSRF_SECRET,),
        ),
        (
            "SKIP_no_cookies",
            "POST",
            True,
            {
                "x-csrf-token": "{}:{}".format(
                    generate_csrf_token(_SAMPLE_USER_ID, ts_now(), _VALID_CSRF_SECRET), ts_now()
                )
            },
            None,
            (_VALID_CSRF_SECRET,),
        ),
        (
            "SKIP_no_user_tokens",
            "POST",
            False,
            {
                "x-csrf-token": "{}:{}".format(
                    generate_csrf_token(_SAMPLE_USER_ID, ts_now(), _VALID_CSRF_SECRET), ts_now()
                )
            },
            {"does_not": "matter"},
            (_VALID_CSRF_SECRET,),
        ),
        (
            "SKIP_view_marker",
            "PUT",
            False,
            {"x-csrf-token": "some_token"},
            {"user_id_cookie": _SAMPLE_USER_ID},
            (_VALID_CSRF_SECRET,),
        ),
        (
            "OK_two_valid_secrets",
            "POST",
            True,
            {
                "x-csrf-token": "{}:{}".format(
                    generate_csrf_token(_SAMPLE_USER_ID, ts_now(), _VALID_CSRF_SECRET), ts_now()
                )
            },
            {"user_id_cookie": _SAMPLE_USER_ID},
            (_VALID_CSRF_SECRET, _ANOTHER_VALID_CSRF_SECRET),
        ),
        (
            "OK_two_valid_secrets_2",
            "POST",
            True,
            {
                "x-csrf-token": "{}:{}".format(
                    generate_csrf_token(_SAMPLE_USER_ID, ts_now(), _ANOTHER_VALID_CSRF_SECRET), ts_now()
                )
            },
            {"user_id_cookie": _SAMPLE_USER_ID},
            (_VALID_CSRF_SECRET, _ANOTHER_VALID_CSRF_SECRET),
        ),
        (
            "OK_multiple_secrets",
            "POST",
            True,
            {
                "x-csrf-token": "{}:{}".format(
                    generate_csrf_token(_SAMPLE_USER_ID, ts_now(), _ANOTHER_VALID_CSRF_SECRET), ts_now()
                )
            },
            {"user_id_cookie": _SAMPLE_USER_ID},
            (_INVALID_CSRF_SECRET, _VALID_CSRF_SECRET, _ANOTHER_VALID_CSRF_SECRET),
        ),
    ],
)
async def test_csrf_ok(
    case_name: str,
    method: str,
    authorized: bool,
    headers: dict[str, str],
    cookies: dict[str, str],
    secrets: tuple[str, ...],
    csrf_app_factory: _AppFactory,
) -> None:
    client = await csrf_app_factory(authorized, secrets)
    resp = await client.request(
        method=method,
        path="/",
        headers=headers,
        cookies=cookies,
    )
    assert resp.status == 200
    resp_json = await resp.json()
    assert resp_json == dict(ok="ok")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case_name, method, authorized, headers, cookies, secrets",
    [
        (
            "INVALID_no_csrf_token_provided",
            "POST",
            False,
            {},
            {"user_id_cookie": _SAMPLE_USER_ID},
            (_VALID_CSRF_SECRET,),
        ),
        (
            "INVALID_malformed_csrf_token_1",
            "POST",
            False,
            {"x-csrf-token": "asdf:1234:5678"},
            {"user_id_cookie": _SAMPLE_USER_ID},
            (_VALID_CSRF_SECRET,),
        ),
        (
            "INVALID_malformed_csrf_token_2",
            "POST",
            False,
            {"x-csrf-token": "asdf:qwer"},
            {"user_id_cookie": _SAMPLE_USER_ID},
            (_VALID_CSRF_SECRET,),
        ),
        (
            "INVALID_empty_token",
            "POST",
            False,
            {"x-csrf-token": ":1234"},
            {"user_id_cookie": _SAMPLE_USER_ID},
            (_VALID_CSRF_SECRET,),
        ),
        (
            "INVALID_token_expired",
            "POST",
            False,
            {
                "x-csrf-token": "{}:{}".format(
                    generate_csrf_token(_SAMPLE_USER_ID, ts_now() - 2 * _CSRF_TIME_LIMIT, _VALID_CSRF_SECRET),
                    ts_now() - 2 * _CSRF_TIME_LIMIT,
                )
            },
            {"user_id_cookie": _SAMPLE_USER_ID},
            (_VALID_CSRF_SECRET,),
        ),
        (
            "INVALID_bad_secret",
            "POST",
            False,
            {
                "x-csrf-token": "{}:{}".format(
                    generate_csrf_token(_SAMPLE_USER_ID, ts_now(), _INVALID_CSRF_SECRET), ts_now()
                )
            },
            {"user_id_cookie": _SAMPLE_USER_ID},
            (_VALID_CSRF_SECRET,),
        ),
        (
            "INVALID_bad_secret_multiple_secrets",
            "POST",
            False,
            {
                "x-csrf-token": "{}:{}".format(
                    generate_csrf_token(_SAMPLE_USER_ID, ts_now(), _INVALID_CSRF_SECRET), ts_now()
                )
            },
            {"user_id_cookie": _SAMPLE_USER_ID},
            (_VALID_CSRF_SECRET, _ANOTHER_VALID_CSRF_SECRET),
        ),
    ],
)
async def test_csrf_invalid(
    case_name: str,
    method: str,
    authorized: bool,
    headers: dict[str, str],
    cookies: dict[str, str],
    secrets: tuple[str, ...],
    csrf_app_factory: _AppFactory,
) -> None:
    validation_failed_text = "CSRF validation failed"
    validation_failed_status = 400

    client = await csrf_app_factory(authorized, secrets)
    resp = await client.request(
        method=method,
        path="/",
        headers=headers,
        cookies=cookies,
    )
    assert resp.status == validation_failed_status
    resp_text = await resp.text()
    assert resp_text == validation_failed_text
