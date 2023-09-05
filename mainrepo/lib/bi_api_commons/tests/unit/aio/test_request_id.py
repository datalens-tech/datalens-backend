from __future__ import annotations

import logging
from typing import Callable, Awaitable, Sequence, Dict

import attr
import pytest
import shortuuid
from aiohttp import web
from aiohttp.test_utils import TestClient
from aiohttp.typedefs import Handler

from bi_api_commons.aio.middlewares.commit_rci import commit_rci_middleware
from bi_api_commons.aio.middlewares.request_bootstrap import RequestBootstrap
from bi_api_commons.aio.middlewares.request_id import RequestId
from bi_api_commons.aiohttp.aiohttp_wrappers import DLRequestBase


pytestmark = [pytest.mark.asyncio]


@attr.s
class _AppConfig:
    request_id: RequestId = attr.ib()
    extra_mw: Sequence = attr.ib(default=())
    extra_handlers: Dict[str, Handler] = attr.ib(factory=dict)


_AppFactory = Callable[[_AppConfig], Awaitable[TestClient]]


@pytest.fixture(scope='function')
async def app_factory(aiohttp_client) -> _AppFactory:
    async def f(config: _AppConfig) -> TestClient:
        async def not_streamed(_):
            return web.json_response({})

        async def streamed(request):
            resp = web.StreamResponse()
            await resp.prepare(request)
            await resp.write_eof(b'ololo')
            return resp

        app = web.Application(
            middlewares=[
                RequestBootstrap(
                    config.request_id,
                ).middleware,
                *config.extra_mw,
            ]
        )
        app.on_response_prepare.append(config.request_id.on_response_prepare)
        app.router.add_get("/not_streamed", not_streamed)
        app.router.add_get("/streamed", streamed)
        for route, handler in config.extra_handlers.items():
            app.router.add_route("*", route, handler)
        return await aiohttp_client(app)

    return f


async def test_no_req_id(app_factory: _AppFactory):
    app = await app_factory(_AppConfig(RequestId()))

    async with app.get("/not_streamed") as resp:
        assert resp.status == 200
        await resp.read()
        assert resp.headers['X-Request-ID']

    async with app.get("/streamed") as resp:
        assert resp.status == 200
        body = await resp.read()
        assert body == b'ololo'
        assert resp.headers['X-Request-ID']

    async with app.get("/not_found_1234125") as resp:
        assert resp.status == 404
        assert resp.headers['X-Request-ID']


async def test_req_id_from_client_no_append(app_factory: _AppFactory):
    app = await app_factory(_AppConfig(RequestId()))
    req_id = shortuuid.uuid()

    async with app.get("/streamed", headers={'x-request-id': req_id}) as resp:
        assert resp.status == 200
        assert resp.headers['X-Request-ID'] == req_id


async def test_req_id_from_client_append(app_factory: _AppFactory):
    app = await app_factory(_AppConfig(RequestId(append_own_req_id=True, app_prefix='sp')))
    req_id = shortuuid.uuid()

    async with app.get("/streamed", headers={'x-request-id': req_id}) as resp:
        assert resp.status == 200
        assert resp.headers['X-Request-ID'].startswith(f"{req_id}--sp.")


async def test_committed_rci(app_factory: _AppFactory):
    async def ensure_rci_committed_handler(request: web.Request) -> web.Response:
        dl_request = DLRequestBase.get_for_request(request)
        return web.json_response({'request_id': dl_request.rci.request_id})

    app = await app_factory(_AppConfig(
        RequestId(),
        extra_mw=(commit_rci_middleware(),),
        extra_handlers={'/ensure_rci': ensure_rci_committed_handler},
    ))

    req_id = shortuuid.uuid()

    async with app.get("/ensure_rci", headers={'x-request-id': req_id}) as resp:
        assert resp.status == 200
        assert resp.headers['X-Request-ID'] == req_id
        resp_json = await resp.json()
        assert resp_json == {'request_id': req_id}


async def test_uncommitted_committed_rci(app_factory: _AppFactory):
    async def ensure_rci_is_not_committed(request: web.Request) -> web.Response:
        dl_request = DLRequestBase.get_for_request(request)
        if dl_request.is_rci_committed():
            return web.json_response({}, status=500, reason="RCI is committed but it was not expected")
        else:
            return web.json_response({'request_id': dl_request.temp_rci.request_id})

    app = await app_factory(_AppConfig(
        RequestId(),
        extra_handlers={'/ensure_rci': ensure_rci_is_not_committed},
    ))

    req_id = shortuuid.uuid()

    async with app.get("/ensure_rci", headers={'x-request-id': req_id}) as resp:
        assert resp.status == 200, resp.reason
        assert resp.headers['X-Request-ID'] == req_id
        resp_json = await resp.json()
        assert resp_json == {'request_id': req_id}


async def test_endpoint_code(app_factory: _AppFactory, caplog):
    caplog.set_level('INFO')

    msg = "hello from dummy view"

    class SomeView(web.View):
        endpoint_code = "some_view_epc"

        async def get(self):
            logging.getLogger().info(msg)
            return web.Response()

    class NoEpc(web.View):
        async def get(self):
            logging.getLogger().info(msg)
            return web.Response()

    app = await app_factory(_AppConfig(
        RequestId(),
        extra_handlers={
            '/some_view': SomeView,
            '/no_epc': NoEpc,
        },
    ))

    # Case 'has endpoint code'
    async with app.get("/some_view") as resp:
        assert resp.status == 200, resp.reason

    log_rec = next(r for r in caplog.records if r.msg == msg)
    assert log_rec.endpoint_code == SomeView.endpoint_code

    # Case 'no endpoint code (not our view)'
    caplog.clear()
    async with app.get('/no_epc') as resp:
        assert resp.status == 200, resp.reason

    log_rec = next(r for r in caplog.records if r.msg == msg)
    assert log_rec.endpoint_code is None


async def test_request_id(app_factory: _AppFactory, caplog):
    caplog.set_level('INFO')

    msg = "hello from dummy view"
    err_msg = "hello from evil view"

    class SomeView(web.View):
        endpoint_code = "some_view_epc"

        async def get(self):
            logging.getLogger().info(msg)
            return web.Response()

    class ErrorView(web.View):
        endpoint_code = "error_view_epc"

        async def get(self):
            logging.getLogger().info(err_msg)
            return web.Response(status=400)

        async def post(self):
            logging.getLogger().info(err_msg)
            raise ValueError('Throwing exception on purpose cause I am evil')

    app = await app_factory(_AppConfig(
        RequestId(append_own_req_id=True, app_prefix='sp'),
        extra_handlers={
            '/some_view': SomeView,
            '/error_view': ErrorView,
        },
    ))

    async with app.get('/some_view', headers={'x-request-id': 'some_req_id'}) as resp:
        assert resp.status == 200, resp.reason
    log_rec = next(r for r in caplog.records if r.msg == msg)
    assert log_rec.request_id.startswith('some_req_id--sp.')
    assert log_rec.parent_request_id == 'some_req_id'
    log_rec = next(r for r in caplog.records if r.msg.startswith('Response'))
    assert log_rec.message == 'Response. method: GET, path: /some_view, status: 200'

    caplog.clear()
    async with app.get('/error_view', headers={'x-request-id': 'some_req_id'}) as resp:
        assert resp.status == 400, resp.reason
    log_rec = next(r for r in caplog.records if r.msg == err_msg)
    assert log_rec.request_id.startswith('some_req_id--sp.')
    assert log_rec.parent_request_id == 'some_req_id'
    log_rec = next(r for r in caplog.records if r.msg.startswith('Response'))
    assert log_rec.message == 'Response. method: GET, path: /error_view, status: 400'

    caplog.clear()
    async with app.post('/error_view', headers={'x-request-id': 'some_req_id'}) as resp:
        assert resp.status == 500, resp.reason
    log_rec = next(r for r in caplog.records if r.msg == err_msg)
    assert log_rec.request_id.startswith('some_req_id--sp.')
    assert log_rec.parent_request_id == 'some_req_id'
    log_rec = next(r for r in caplog.records if r.msg.startswith('Response'))
    assert log_rec.message == 'Response. method: POST, path: /error_view, status: 500'
