from __future__ import annotations

from aiohttp import web
import pytest

from bi_api_commons_ya_team.aio.middlewares.blackbox_auth import blackbox_auth_middleware
from dl_api_commons.aio.middlewares.commit_rci import commit_rci_middleware
from dl_api_commons.aio.middlewares.request_bootstrap import RequestBootstrap
from dl_api_commons.aio.middlewares.request_id import RequestId
from dl_api_commons.aiohttp.aiohttp_wrappers import (
    DLRequestView,
    RequiredResourceCommon,
)
from dl_constants.api_constants import DLHeadersCommon


pytestmark = [pytest.mark.asyncio]


@pytest.fixture
def client(loop, aiohttp_client, tvm_info):
    app = web.Application(
        middlewares=[
            RequestBootstrap(
                req_id_service=RequestId(),
            ).middleware,
            blackbox_auth_middleware(tvm_info=tvm_info),
            commit_rci_middleware(),
        ]
    )

    class TestView(DLRequestView):
        async def get(self):
            return web.json_response(
                {
                    "user_id": self.dl_request.user_id,
                    "user_name": self.dl_request.user_name,
                }
            )

    class NoAuthView(DLRequestView):
        REQUIRED_RESOURCES = frozenset({RequiredResourceCommon.SKIP_AUTH})

        async def get(self):  # noqa
            return web.json_response({})

    app.router.add_route("*", "/", TestView)
    app.router.add_route("*", "/no_auth", NoAuthView)

    return loop.run_until_complete(aiohttp_client(app))


@pytest.mark.xfail
@pytest.mark.asyncio
async def test_auth_ok(client, intranet_user_1_creds):
    resp = await client.get(
        "/",
        headers={
            DLHeadersCommon.AUTHORIZATION_TOKEN.value: f"OAuth {intranet_user_1_creds.token}",
        },
    )
    assert resp.status == 200
    data = await resp.json()

    assert data["user_id"] is not None and data["user_name"] is not None
    # TODO FIX: Fill when data will be stored in test_intranet_user_creds
    assert data == {
        "user_id": str(intranet_user_1_creds.user_id),
        "user_name": intranet_user_1_creds.user_name,
    }


@pytest.mark.asyncio
async def test_auth_bearer_token_not_accepted(client, test_intranet_user_creds_wrong_scope):
    resp = await client.get(
        "/",
        headers={
            DLHeadersCommon.AUTHORIZATION_TOKEN.value: f"Bearer {test_intranet_user_creds_wrong_scope.token}",
        },
    )
    assert resp.status == 401


@pytest.mark.asyncio
async def test_auth_invalid_scope(client, test_intranet_user_creds_wrong_scope):
    resp = await client.get(
        "/",
        headers={
            DLHeadersCommon.AUTHORIZATION_TOKEN.value: f"OAuth {test_intranet_user_creds_wrong_scope.token}",
        },
    )
    assert resp.status == 403


@pytest.mark.asyncio
async def test_auth_invalid_token(client):
    resp = await client.get(
        "/",
        headers={
            DLHeadersCommon.AUTHORIZATION_TOKEN.value: "OAuth asdQWERtype",
        },
    )
    assert resp.status == 403


@pytest.mark.asyncio
async def test_auth_no_token(client):
    resp = await client.get("/")
    assert resp.status == 403


@pytest.mark.asyncio
async def test_auth_no_auth(client):
    resp = await client.get("/no_auth")
    assert resp.status == 200
