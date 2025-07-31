from aiohttp import web
from aiohttp.test_utils import TestClient
from multidict import CIMultiDict
import pytest

from dl_api_commons.aio.middlewares.commit_rci import commit_rci_middleware
from dl_api_commons.aio.middlewares.error_handling_outer import (
    AIOHTTPErrorHandler,
    ErrorData,
    ErrorLevel,
)
from dl_api_commons.aio.middlewares.request_bootstrap import RequestBootstrap
from dl_api_commons.aio.middlewares.request_id import RequestId
from dl_api_commons.aio.server_header import ServerHeader
from dl_api_commons.aiohttp.aiohttp_wrappers import DLRequestView


@pytest.mark.asyncio
async def test_service_header_normal_case(aiohttp_client: TestClient) -> None:
    server_header = "Ololo"

    class SimpleView(web.View):
        async def get(self) -> web.Response:
            return web.json_response({})

    class FailView(web.View):
        async def get(self) -> web.Response:
            raise ValueError()

    class StreamedView(web.View):
        async def get(self) -> web.StreamResponse:
            streamed_resp = web.StreamResponse()
            await streamed_resp.prepare(self.request)
            await streamed_resp.write(b"ololo")
            return streamed_resp

    app = web.Application()
    app.router.add_route("*", "/simple_view", SimpleView)
    app.router.add_route("*", "/fail_view", FailView)
    app.router.add_route("*", "/streamed_view", StreamedView)

    ServerHeader(server_header).add_signal_handlers(app)

    client = await aiohttp_client(app)  # type: ignore[operator]

    resp = await client.get("/not_found")
    assert 404 == resp.status and [server_header] == resp.headers.getall("Server")

    resp = await client.get("/simple_view")
    assert 200 == resp.status and [server_header] == resp.headers.getall("Server")

    resp = await client.get("/fail_view")
    assert 500 == resp.status and [server_header] == resp.headers.getall("Server")

    resp = await client.get("/streamed_view")
    assert 200 == resp.status and [server_header] == resp.headers.getall("Server")


def test_service_header_validation_fail() -> None:
    with pytest.raises(ValueError):
        ServerHeader("")

    with pytest.raises(TypeError):
        ServerHeader(123)  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_commit_rci_middleware(caplog: pytest.LogCaptureFixture, aiohttp_client: TestClient) -> None:
    caplog.set_level("INFO")

    app = web.Application(
        middlewares=[
            RequestBootstrap(
                req_id_service=RequestId(),
            ).middleware,
            commit_rci_middleware(
                rci_extra_plain_headers=("X-Add-Me", "x-add-me-2"), rci_extra_secret_headers=("X-Add-Me-Secret",)
            ),
        ]
    )

    class TestView(DLRequestView):
        async def get(self) -> web.Response:
            rci = self.dl_request.rci
            return web.json_response(
                {
                    "secret_headers": [[k, v] for k, v in rci.secret_headers.items()],
                    "plain_headers": [[k, v] for k, v in rci.plain_headers.items()],
                    "req_id": rci.request_id,
                }
            )

    app.router.add_route("*", "/simple_view", TestView)
    client = await aiohttp_client(app)  # type: ignore[operator]

    async def get_headers(inbound_headers: CIMultiDict) -> dict:
        resp = await client.get("/simple_view", headers=inbound_headers, skip_auto_headers=("User-Agent",))
        try:
            assert resp.status == 200
            return await resp.json()
        finally:
            resp.close()

    actual_headers = await get_headers(
        CIMultiDict(
            (
                ("X-Add-Me", "plain_val"),
                ("X-Ignore-Me", "some"),
                ("X-Add-Me-Secret", "secret_val"),
                (
                    "X-Request-Id",
                    "reqid1",
                ),
                ("X-Chart-Id", "some_chart_id"),
                ("Host", "127.0.0.1"),
                ("Accept-Language", "en"),
            )
        )
    )

    assert actual_headers == {
        "secret_headers": [["X-Add-Me-Secret", "secret_val"]],
        "plain_headers": [
            ["Host", "127.0.0.1"],
            ["X-Add-Me", "plain_val"],
            ["X-Chart-Id", "some_chart_id"],
            ["Accept-Language", "en"],
        ],
        "req_id": "reqid1",
    }

    actual_headers = await get_headers(
        CIMultiDict(
            (
                ("x-add-me", "plain_val"),
                ("x-ignore-me", "some"),
                ("x-add-me-secret", "secret_val"),
                (
                    "x-request-id",
                    "reqid2",
                ),
                ("x-chart-id", "some_chart_id"),
                ("host", "127.0.0.1"),
                ("Accept-Language", "en"),
            )
        )
    )

    assert actual_headers == {
        "secret_headers": [["x-add-me-secret", "secret_val"]],
        "plain_headers": [
            ["Host", "127.0.0.1"],
            ["x-add-me", "plain_val"],
            ["x-chart-id", "some_chart_id"],
            ["Accept-Language", "en"],
        ],
        "req_id": "reqid2",
    }


@pytest.mark.asyncio
async def test_error_handling_middleware(
    caplog: pytest.LogCaptureFixture,
    aiohttp_client: TestClient,
) -> None:
    caplog.set_level("INFO")
    log_name = "dl_api_commons.aio.middlewares.error_handling_outer"

    class MyException(Exception):
        def __init__(self, msg: str, level: str, status_code: int):
            super().__init__(msg)
            self.msg = msg
            self.level = level
            self.status_code = status_code

    class ErrorHandler(AIOHTTPErrorHandler):
        def _classify_error(self, err: Exception, request: web.Request) -> ErrorData:
            if isinstance(err, MyException):
                return ErrorData(
                    status_code=err.status_code,
                    level=ErrorLevel[err.level],
                    response_body=dict(msg=err.msg),
                )
            if isinstance(err, web.HTTPException):
                return ErrorData(
                    status_code=err.status_code,
                    level=ErrorLevel.info,
                    response_body={},
                    http_reason=err.reason,
                )
            else:
                raise ValueError("Can not format exception")

    async def info(request: web.Request) -> web.Response:
        raise MyException("info_exc", "info", 400)

    async def warn(request: web.Request) -> web.Response:
        raise MyException("warn_exc", "warning", 400)

    async def error(request: web.Request) -> web.Response:
        raise MyException("err_exc", "error", 500)

    async def fmt_error(request: web.Request) -> web.Response:
        raise ValueError()

    async def ok(request: web.Request) -> web.Response:
        return web.json_response({})

    app = web.Application(
        middlewares=[
            RequestBootstrap(
                req_id_service=RequestId(),
                error_handler=ErrorHandler(sentry_app_name_tag=None),
            ).middleware,
        ]
    )
    app.router.add_get("/ok", ok)
    app.router.add_get("/info", info)
    app.router.add_get("/warn", warn)
    app.router.add_get("/error", error)
    app.router.add_get("/fmt_error", fmt_error)

    client = await aiohttp_client(app)  # type: ignore[operator]

    async def get_status_and_body(url: str) -> tuple[int, dict]:
        resp = await client.get(url)
        return resp.status, await resp.json()

    assert await get_status_and_body("/ok") == (200, {})

    caplog.clear()
    assert await get_status_and_body("/info") == (400, {"msg": "info_exc"})
    log_rec = [r for r in caplog.records if r.message == "Regular exception fired" and r.name == log_name][0]
    assert log_rec.levelname == "INFO"
    assert log_rec.exc_info

    caplog.clear()
    assert await get_status_and_body("/warn") == (400, {"msg": "warn_exc"})
    log_rec = [r for r in caplog.records if r.levelname == "WARNING" and r.name == log_name][0]
    assert log_rec.message == "Warning exception fired"
    assert log_rec.exc_info

    caplog.clear()
    assert await get_status_and_body("/error") == (500, {"msg": "err_exc"})
    log_rec = [r for r in caplog.records if r.levelname == "ERROR" and r.name == log_name][0]
    assert log_rec.message == "Caught an exception in request handler"
    assert log_rec.exc_info

    caplog.clear()
    assert await get_status_and_body("/fmt_error") == (500, {"message": "Internal Server Error"})
    fmt_log_rec = [r for r in caplog.records if r.levelname == "CRITICAL" and r.name == log_name][0]
    cause_log_rec = [r for r in caplog.records if r.levelname == "ERROR" and r.name == log_name][0]

    assert fmt_log_rec.message == "Error handler raised an error during creating error response"
    assert fmt_log_rec.exc_info

    assert cause_log_rec.message == "Error handler raised an error during handling this exception"
    assert cause_log_rec.exc_info
    assert cause_log_rec.exc_info != fmt_log_rec.exc_info

    caplog.clear()
    resp = await client.get("/not_found")
    resp_json = await resp.json()
    assert resp_json == {}
    assert resp.reason == "Not Found"
    log_rec = [r for r in caplog.records if r.message == "Regular exception fired" and r.name == log_name][0]
    assert log_rec.levelname == "INFO"
    assert log_rec.exc_info
