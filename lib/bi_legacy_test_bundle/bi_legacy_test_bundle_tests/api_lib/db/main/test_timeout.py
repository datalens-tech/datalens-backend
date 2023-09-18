import asyncio

from aiohttp import web
import pytest

from dl_api_commons.aio.middlewares.request_bootstrap import RequestBootstrap
from dl_api_commons.aio.middlewares.request_id import RequestId
from dl_api_commons.aiohttp.aiohttp_wrappers import DLRequestView
from dl_api_lib.aio.middlewares.error_handling_outer import DatasetAPIErrorHandler


@pytest.mark.asyncio
async def test_request_bootstrap_timeout(aiohttp_client):
    common_timeout = 0.5

    app = web.Application(
        middlewares=[
            RequestBootstrap(
                req_id_service=RequestId(),
                error_handler=DatasetAPIErrorHandler(
                    public_mode=False,
                    use_sentry=False,
                    sentry_app_name_tag="",
                ),
                timeout_sec=common_timeout,
            ).middleware,
        ]
    )

    class TestView(DLRequestView):
        async def get(self):
            to_sleep = float(self.request.rel_url.query["time_to_sleep"])
            await asyncio.sleep(to_sleep)
            return web.json_response({})

    app.router.add_route("*", "/sleeping_view", TestView)
    client = await aiohttp_client(app)

    resp = await client.get("/sleeping_view", params={"time_to_sleep": common_timeout + 0.1})
    try:
        assert resp.status == 424
        resp_json = await resp.json()
        assert resp_json["code"] == "ERR.DS_API.REQUEST_TIMEOUT"
    finally:
        resp.close()

    resp = await client.get("/sleeping_view", params={"time_to_sleep": common_timeout - 0.1})
    try:
        assert resp.status == 200
    finally:
        resp.close()
