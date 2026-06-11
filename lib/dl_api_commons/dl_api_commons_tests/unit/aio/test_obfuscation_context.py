import asyncio
import json
import logging
from typing import Any

from aiohttp import web
from aiohttp.typedefs import (
    Handler,
    Middleware,
)
import attr
from multidict import CIMultiDict
import pytest

from dl_api_commons.aio.middlewares.commit_rci import commit_rci_middleware
from dl_api_commons.aio.middlewares.obfuscation_context import obfuscation_context_middleware
from dl_api_commons.aio.middlewares.request_bootstrap import RequestBootstrap
from dl_api_commons.aio.middlewares.request_id import RequestId
from dl_api_commons.aiohttp.aiohttp_wrappers import DLRequestBase
import dl_auth
import dl_constants
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


@attr.s
class _AuthDataStub(dl_auth.AuthData):
    oauth_token: str = attr.ib(repr=False)
    public_id: str = attr.ib(default="")

    def get_headers(self, target: dl_auth.AuthTarget | None = None) -> dict[dl_constants.DLHeaders, str]:
        return {}

    def get_cookies(self, target: dl_auth.AuthTarget | None = None) -> dict[dl_constants.DLCookies, str]:
        return {}


def _seeding_middleware(
    secret_headers: CIMultiDict | None = None,
    auth_data: dl_auth.AuthData | None = None,
    set_auth_data: bool = False,
) -> Middleware:
    """Test helper: inject secret_headers / auth_data into temp_rci before obfuscation_context_middleware runs."""

    @web.middleware
    @DLRequestBase.use_dl_request
    async def actual(dl_request: DLRequestBase, handler: Handler) -> web.StreamResponse:
        kwargs: dict[str, Any] = {}
        if secret_headers is not None:
            kwargs["secret_headers"] = secret_headers
        if set_auth_data:
            kwargs["auth_data"] = auth_data
        if kwargs:
            dl_request.update_temp_rci(**kwargs)
        return await handler(dl_request.request)

    return actual


async def _capture_keeper_secrets_handler(request: web.Request) -> web.Response:
    """Handler that returns the committed RCI's secret_keeper secrets as a JSON-like string."""
    dl_request = DLRequestBase.get_for_request(request)
    assert dl_request is not None
    rci = dl_request.rci
    return web.Response(text=json.dumps(rci.secret_keeper.secrets))


def _build_seeded_app(
    secret_headers: CIMultiDict | None = None,
    auth_data: dl_auth.AuthData | None = None,
    set_auth_data: bool = False,
) -> web.Application:
    global_keeper = SecretKeeper()
    global_keeper.add_secret("GLOBAL_TOKEN", "global_token")

    app = web.Application(
        middlewares=[
            RequestBootstrap(RequestId()).middleware,
            _seeding_middleware(secret_headers=secret_headers, auth_data=auth_data, set_auth_data=set_auth_data),
            obfuscation_context_middleware(),
            commit_rci_middleware(),
        ],
    )
    app[OBFUSCATION_BASE_OBFUSCATORS_KEY] = create_base_obfuscators(global_keeper=global_keeper)
    app.router.add_get("/secrets", _capture_keeper_secrets_handler)
    return app


class TestMiddlewareSecretPopulationAio:
    @pytest.mark.asyncio
    async def test_secret_headers_added_to_keeper(self, aiohttp_client: Any) -> None:
        rci_secret_headers = CIMultiDict(
            [
                ("Authorization", "Bearer secret-bearer-token-XXXX"),
                ("Cookie", "Session_id=long-cookie-value-XXXX"),
            ]
        )
        client = await aiohttp_client(_build_seeded_app(secret_headers=rci_secret_headers))

        async with client.get("/secrets") as resp:
            assert resp.status == 200
            secrets = json.loads(await resp.text())

        assert secrets.get("Bearer secret-bearer-token-XXXX") == "header.Authorization"
        assert secrets.get("Session_id=long-cookie-value-XXXX") == "header.Cookie"

    @pytest.mark.asyncio
    async def test_auth_data_added_to_keeper(self, aiohttp_client: Any) -> None:
        auth_data = _AuthDataStub(oauth_token="oauth-token-value-XXXX", public_id="user-123")
        client = await aiohttp_client(_build_seeded_app(auth_data=auth_data, set_auth_data=True))

        async with client.get("/secrets") as resp:
            assert resp.status == 200
            secrets = json.loads(await resp.text())

        assert secrets.get("oauth-token-value-XXXX") == "auth_data.oauth_token"
        # `public_id` has default repr=True (not a secret marker) -> must NOT be in the keeper.
        assert "user-123" not in secrets

    @pytest.mark.asyncio
    async def test_no_auth_data_no_crash(self, aiohttp_client: Any) -> None:
        rci_secret_headers = CIMultiDict([("Authorization", "Bearer only-header-value-YYYY")])
        client = await aiohttp_client(
            _build_seeded_app(
                secret_headers=rci_secret_headers,
                auth_data=None,
                set_auth_data=True,
            )
        )

        async with client.get("/secrets") as resp:
            assert resp.status == 200
            secrets = json.loads(await resp.text())

        # No exception thrown; header still made it in, no auth_data.* keys present.
        assert secrets.get("Bearer only-header-value-YYYY") == "header.Authorization"
        assert not any(name.startswith("auth_data.") for name in secrets.values())
