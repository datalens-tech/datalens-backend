import asyncio
import logging
from typing import Any

from aiohttp import web
import pytest

from dl_api_commons.aio.middlewares.commit_rci import commit_rci_middleware
from dl_api_commons.aio.middlewares.obfuscation_context import obfuscation_context_middleware
from dl_api_commons.aio.middlewares.request_bootstrap import RequestBootstrap
from dl_api_commons.aio.middlewares.request_id import RequestId
from dl_api_commons.aiohttp.aiohttp_wrappers import DLRequestBase
from dl_logging.format import StdoutFormatter
from dl_obfuscator import (
    OBFUSCATION_BASE_OBFUSCATORS_KEY,
    ObfuscationContext,
    create_base_obfuscators,
    get_request_obfuscation_engine,
)
from dl_obfuscator.profiling import (
    LogFormatProfilingContext,
    get_log_format_profiling,
)
from dl_obfuscator.secret_keeper import SecretKeeper


async def _echo_handler(request: web.Request) -> web.Response:
    text = request.query.get("text", "")
    engine = get_request_obfuscation_engine()
    if engine is not None:
        text = engine.obfuscate(text, ObfuscationContext.LOGS)
    return web.Response(text=text)


async def _echo_with_secret_handler(request: web.Request) -> web.Response:
    text = request.query.get("text", "")
    secret = request.query.get("secret", "")

    dl_request = DLRequestBase.get_for_request(request)
    if dl_request is not None:
        rci = dl_request.last_resort_rci
        if rci is not None and secret:
            rci.secret_keeper.add_secret(secret, "req_secret")

    engine = get_request_obfuscation_engine()
    if engine is not None:
        text = engine.obfuscate(text, ObfuscationContext.LOGS)
    return web.Response(text=text)


async def _rci_engine_handler(request: web.Request) -> web.Response:
    """Handler that checks the committed RCI has an obfuscation engine."""
    dl_request = DLRequestBase.get_for_request(request)
    assert dl_request is not None
    rci = dl_request.rci
    has_engine = rci.obfuscation_engine is not None
    return web.Response(text=str(has_engine))


def _create_test_app() -> web.Application:
    global_keeper = SecretKeeper()
    global_keeper.add_secret("GLOBAL_TOKEN", "global_token")

    app = web.Application(
        middlewares=[
            RequestBootstrap(RequestId()).middleware,
            obfuscation_context_middleware(),
        ],
    )
    app[OBFUSCATION_BASE_OBFUSCATORS_KEY] = create_base_obfuscators(global_keeper=global_keeper)
    app.router.add_get("/echo", _echo_handler)
    app.router.add_get("/echo-secret", _echo_with_secret_handler)
    app.router.add_get("/rci-engine", _rci_engine_handler)
    return app


def _create_test_app_with_commit() -> web.Application:
    global_keeper = SecretKeeper()
    global_keeper.add_secret("GLOBAL_TOKEN", "global_token")

    app = web.Application(
        middlewares=[
            RequestBootstrap(RequestId()).middleware,
            obfuscation_context_middleware(),
            commit_rci_middleware(),
        ],
    )
    app[OBFUSCATION_BASE_OBFUSCATORS_KEY] = create_base_obfuscators(global_keeper=global_keeper)
    app.router.add_get("/echo", _echo_handler)
    app.router.add_get("/rci-engine", _rci_engine_handler)
    return app


@pytest.mark.asyncio
async def test_obfuscation_middleware_applies_global_secret(aiohttp_client: Any) -> None:
    client = await aiohttp_client(_create_test_app())

    async with client.get("/echo", params={"text": "GLOBAL_TOKEN visible"}) as resp:
        assert resp.status == 200
        body = await resp.text()
        assert "GLOBAL_TOKEN" not in body
        assert "***global_token***" in body
        assert "visible" in body


@pytest.mark.asyncio
async def test_concurrent_requests_obfuscation_isolation(aiohttp_client: Any) -> None:
    client = await aiohttp_client(_create_test_app())
    request_count = 5
    secrets = [f"REQUEST_SECRET_{i}" for i in range(request_count)]
    all_secrets_text = " ".join(secrets)

    async def send_request(i: int) -> str:
        # Text contains the global token AND all per-request secrets.
        # Only the global token and this request's own secret should be obfuscated.
        text = f"GLOBAL_TOKEN {all_secrets_text}"
        resp = await client.get("/echo-secret", params={"text": text, "secret": secrets[i]})
        return await resp.text()

    results = await asyncio.gather(*[send_request(i) for i in range(request_count)])

    for i, result in enumerate(results):
        # Global secret must be obfuscated
        assert "GLOBAL_TOKEN" not in result, f"Request {i} leaked global secret"
        assert "***global_token***" in result

        # Own per-request secret must be obfuscated
        assert secrets[i] not in result, f"Request {i} did not obfuscate its own secret"
        assert "***req_secret***" in result

        # Other requests' secrets must NOT be obfuscated (they are not secrets for this request)
        for j in range(request_count):
            if j == i:
                continue
            assert secrets[j] in result, f"Request {i} incorrectly obfuscated request {j}'s secret"


@pytest.mark.asyncio
async def test_committed_rci_has_obfuscation_engine(aiohttp_client: Any) -> None:
    client = await aiohttp_client(_create_test_app_with_commit())
    async with client.get("/rci-engine") as resp:
        assert resp.status == 200
        text = await resp.text()
        assert text == "True", "Committed RCI should have obfuscation_engine set"


def _create_test_app_no_obfuscation() -> web.Application:
    app = web.Application(
        middlewares=[
            RequestBootstrap(RequestId()).middleware,
            obfuscation_context_middleware(),
        ],
    )
    app.router.add_get("/echo", _echo_handler)
    return app


@pytest.mark.asyncio
async def test_profiling_disabled_by_default(aiohttp_client: Any) -> None:
    profiling_seen: list[LogFormatProfilingContext | None] = []

    async def capture_handler(request: web.Request) -> web.Response:
        profiling_seen.append(get_log_format_profiling())
        return web.Response(text="ok")

    app = web.Application(
        middlewares=[
            RequestBootstrap(RequestId()).middleware,
            obfuscation_context_middleware(),  # default: profiling off
        ],
    )
    app.router.add_get("/capture", capture_handler)

    client = await aiohttp_client(app)
    async with client.get("/capture") as resp:
        assert resp.status == 200

    assert profiling_seen[0] is None


@pytest.mark.asyncio
async def test_profiling_not_emitted_when_count_is_zero(
    aiohttp_client: Any,
    caplog: pytest.LogCaptureFixture,
) -> None:
    app = web.Application(
        middlewares=[
            RequestBootstrap(RequestId()).middleware,
            obfuscation_context_middleware(log_format_profiling_enabled=True),
        ],
    )
    app.router.add_get("/echo", _echo_handler)

    client = await aiohttp_client(app)
    with caplog.at_level(logging.INFO, logger="dl_obfuscator.profiling"):
        async with client.get("/echo", params={"text": "hello"}) as resp:
            assert resp.status == 200

    profiling_records = [r for r in caplog.records if r.name == "dl_obfuscator.profiling"]
    assert len(profiling_records) == 0  # call_count stays 0 (no StdoutFormatter in test app)


@pytest.mark.asyncio
async def test_profiling_emitted_when_formatter_active(
    aiohttp_client: Any,
    caplog: pytest.LogCaptureFixture,
) -> None:
    class _FormattingHandler(logging.Handler):
        def __init__(self) -> None:
            super().__init__()
            self.setFormatter(StdoutFormatter())

        def emit(self, record: logging.LogRecord) -> None:
            self.format(record)

    test_logger = logging.getLogger("test_profiling_formatter_aio")
    handler = _FormattingHandler()
    test_logger.addHandler(handler)
    test_logger.setLevel(logging.DEBUG)
    test_logger.propagate = False

    async def logging_handler(request: web.Request) -> web.Response:
        test_logger.info("request handled")
        return web.Response(text="ok")

    app = web.Application(
        middlewares=[
            RequestBootstrap(RequestId()).middleware,
            obfuscation_context_middleware(log_format_profiling_enabled=True),
        ],
    )
    app.router.add_get("/log", logging_handler)

    try:
        client = await aiohttp_client(app)
        with caplog.at_level(logging.INFO, logger="dl_obfuscator.profiling"):
            async with client.get("/log") as resp:
                assert resp.status == 200
    finally:
        test_logger.removeHandler(handler)

    profiling_records = [r for r in caplog.records if r.name == "dl_obfuscator.profiling"]
    assert len(profiling_records) == 1
