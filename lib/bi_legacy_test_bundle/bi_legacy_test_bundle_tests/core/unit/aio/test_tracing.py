from __future__ import annotations

from typing import Callable, Optional

import pytest
from aiohttp import web

from dl_api_commons.aio.middlewares.error_handling_outer import AIOHTTPErrorHandler, ErrorData, ErrorLevel
from dl_api_commons.aio.middlewares.request_bootstrap import RequestBootstrap
from dl_api_commons.aio.middlewares.request_id import RequestId

from dl_core.aio.middlewares.tracing import TracingService


class MockError(Exception):
    body: dict
    status: int
    level: ErrorLevel

    def __init__(self, status: int, level: ErrorLevel, body: dict):
        super().__init__(status, level)
        self.status = status
        self.level = level
        self.body = body


class MockErrorHandlingMW(AIOHTTPErrorHandler):
    def _classify_error(self, err: Exception, request: web.Request) -> ErrorData:
        if isinstance(err, MockError):
            return ErrorData(err.status, err.body, err.level)
        return ErrorData(500, {}, ErrorLevel.error)


def get_view(the_endpoint_code: Optional[str], action: Callable[[], dict]):
    if the_endpoint_code is not None:
        class EPView(web.View):
            endpoint_code = the_endpoint_code

            async def get(self):
                return web.json_response(action())

        return EPView
    else:
        class NonEPView(web.View):
            async def get(self):
                return web.json_response(action())

        return NonEPView


def register_view(app: web.Application, path: str, ep_code: Optional[str]):
    def deco(func: Callable[[], dict]):
        app.router.add_route('*', path, get_view(ep_code, func))
        return func

    return deco


@pytest.mark.asyncio
async def test_tracing_scenarios(aiohttp_client):
    """
    At this moment only checks that tracing/error handling middleware doesn't breaks something.
    TODO: Add InMemory reporter to tests tracer to be able to check that spans got correct tags
    """
    app = web.Application(middlewares=[
        TracingService().middleware,
        RequestBootstrap(
            req_id_service=RequestId(),
            error_handler=MockErrorHandlingMW(use_sentry=False, sentry_app_name_tag='tests'),
        ).middleware,
    ])

    @register_view(app, "/ok", "ok")
    def ok():
        return dict(ok='ok')

    @register_view(app, "/err_err", "err")
    def err_err():
        raise MockError(200, ErrorLevel.error, dict(err_err=None))

    @register_view(app, "/err_info", "err_info")
    def err_info():
        raise MockError(400, ErrorLevel.info, dict(err_info=None))

    @register_view(app, "/no_ep_ok", None)
    def no_ep_ok():
        return dict(no_ep_ok=None)

    @register_view(app, "/no_ep_err", None)
    def no_ep_err():
        raise MockError(401, ErrorLevel.error, dict(no_ep_err=None))

    client = await aiohttp_client(app)

    resp = await client.get("/ok")
    resp_json = await resp.json()
    assert resp.status == 200 and resp_json == dict(ok='ok')

    resp = await client.get("/err_err")
    resp_json = await resp.json()
    assert resp.status == 200 and resp_json == dict(err_err=None)

    resp = await client.get("/err_info")
    resp_json = await resp.json()
    assert resp.status == 400 and resp_json == dict(err_info=None)

    resp = await client.get("/no_ep_ok")
    resp_json = await resp.json()
    assert resp.status == 200 and resp_json == dict(no_ep_ok=None)

    resp = await client.get("/no_ep_err")
    resp_json = await resp.json()
    assert resp.status == 401 and resp_json == dict(no_ep_err=None)
