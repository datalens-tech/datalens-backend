from __future__ import annotations

import uuid
from asyncio import Future

import redis.asyncio
from aiohttp import web
import pytest

from dl_api_commons.aio.middlewares.commit_rci import commit_rci_middleware
from dl_api_commons.aio.middlewares.request_bootstrap import RequestBootstrap
from dl_api_commons.aio.middlewares.request_id import RequestId
from dl_api_commons.aiohttp.aiohttp_wrappers import RequiredResourceCommon

from dl_core.aio.aiohttp_wrappers_data_core import DLRequestDataCore
from dl_core.aio.middlewares.us_manager import service_us_manager_middleware
from dl_core.aio.web_app_services.redis import SingleHostSimpleRedisService
from dl_constants.enums import RedisInstanceKind


@pytest.mark.asyncio
async def test_redis_service(qc_redis_url, aiohttp_client, core_test_config):
    us_config = core_test_config.get_us_config()
    app = web.Application(middlewares=[
        RequestBootstrap(
            req_id_service=RequestId(
                dl_request_cls=DLRequestDataCore,
            ),
        ).middleware,
        commit_rci_middleware(),
        service_us_manager_middleware(
            # TODO FIX: Use fixtures instead of env
            us_base_url=us_config.us_host,
            crypto_keys_config=core_test_config.get_crypto_keys_config(),
            us_master_token=us_config.us_master_token,
        )
    ])

    redis_service = SingleHostSimpleRedisService(
        instance_kind=RedisInstanceKind.caches,
        url=qc_redis_url,
    )
    app.on_startup.append(redis_service.init_hook)
    app.on_cleanup.append(redis_service.tear_down_hook)

    dl_request_fut = Future()

    class HomeView(web.View):
        REQUIRED_RESOURCES = frozenset({RequiredResourceCommon.SERVICE_US_MANAGER})

        async def get(self):
            dl_request_fut.set_result(self.request[DLRequestDataCore.KEY_DL_REQUEST])
            return web.json_response({})

    app.router.add_route('*', '/', HomeView)
    client = await aiohttp_client(app)
    resp = await client.get("/")
    assert resp.status == 200

    dl_request: DLRequestDataCore = await dl_request_fut

    redis_cli = dl_request.get_caches_redis()
    assert isinstance(redis_cli, redis.asyncio.Redis)

    key = f"test_redis_service:{uuid.uuid4().hex}"
    val = uuid.uuid4().hex.encode('ascii')

    await redis_cli.set(key, val)
    fetched = await redis_cli.get(key)
    await redis_cli.delete(key)

    assert fetched == val
