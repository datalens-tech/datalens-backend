import logging

import attr
import flask
from multidict import CIMultiDict
import pytest

import dl_api_commons.flask.middlewares as dl_api_commons_flask_middlewares
from dl_api_commons.flask.middlewares.commit_rci_middleware import ReqCtxInfoMiddleware
from dl_api_commons.flask.middlewares.obfuscation_context import setup_obfuscation_context_middleware
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


def _create_test_app() -> flask.Flask:
    global_keeper = SecretKeeper()
    global_keeper.add_secret("GLOBAL_TOKEN", "global_token")

    app = flask.Flask(__name__)
    dl_api_commons_flask_middlewares.ContextVarMiddleware().wrap_flask_app(app)
    dl_api_commons_flask_middlewares.RequestLoggingContextControllerMiddleWare().set_up(app)
    dl_api_commons_flask_middlewares.RequestIDService(
        request_id_app_prefix=None,
        append_local_req_id=False,
    ).set_up(app)
    dl_api_commons_flask_middlewares.RCIHeadersMiddleware().set_up(app)

    app.config[OBFUSCATION_BASE_OBFUSCATORS_KEY] = create_base_obfuscators(global_keeper=global_keeper)
    setup_obfuscation_context_middleware(app)

    dl_api_commons_flask_middlewares.ReqCtxInfoMiddleware().set_up(app)

    return app


def _create_test_app_no_obfuscation() -> flask.Flask:
    app = flask.Flask(__name__)
    dl_api_commons_flask_middlewares.ContextVarMiddleware().wrap_flask_app(app)
    dl_api_commons_flask_middlewares.RequestLoggingContextControllerMiddleWare().set_up(app)
    dl_api_commons_flask_middlewares.RequestIDService(
        request_id_app_prefix=None,
        append_local_req_id=False,
    ).set_up(app)
    dl_api_commons_flask_middlewares.RCIHeadersMiddleware().set_up(app)

    # No OBFUSCATION_BASE_OBFUSCATORS_KEY set, but middleware is registered (should be no-op)
    setup_obfuscation_context_middleware(app)

    dl_api_commons_flask_middlewares.ReqCtxInfoMiddleware().set_up(app)

    return app


def test_obfuscation_middleware_applies_global_secret() -> None:
    app = _create_test_app()
    results: list[str] = []

    @app.route("/echo")
    def echo() -> flask.Response:
        text = flask.request.args.get("text", "")
        engine = get_request_obfuscation_engine()
        if engine is not None:
            text = engine.obfuscate(text, ObfuscationContext.LOGS)
        results.append(text)
        return flask.jsonify({"text": text})

    client = app.test_client()
    resp = client.get("/echo", query_string={"text": "GLOBAL_TOKEN visible"})
    assert resp.status_code == 200

    assert len(results) == 1
    assert "GLOBAL_TOKEN" not in results[0]
    assert "***global_token***" in results[0]
    assert "visible" in results[0]


def test_committed_rci_has_obfuscation_engine() -> None:
    app = _create_test_app()
    results: list[bool] = []

    @app.route("/rci-engine")
    def rci_engine() -> flask.Response:
        rci = dl_api_commons_flask_middlewares.ReqCtxInfoMiddleware.get_request_context_info()
        has_engine = rci.obfuscation_engine is not None
        results.append(has_engine)
        return flask.jsonify({"has_engine": has_engine})

    client = app.test_client()
    resp = client.get("/rci-engine")
    assert resp.status_code == 200

    assert len(results) == 1
    assert results[0] is True, "Committed RCI should have obfuscation_engine set"


def test_contextvar_cleared_after_request() -> None:
    app = _create_test_app()

    @app.route("/echo")
    def echo() -> flask.Response:
        return flask.jsonify({})

    client = app.test_client()
    resp = client.get("/echo")
    assert resp.status_code == 200

    # After request, the ContextVar should be cleared
    assert get_request_obfuscation_engine() is None


def test_no_op_when_base_obfuscators_absent() -> None:
    app = _create_test_app_no_obfuscation()
    results: list[bool] = []

    @app.route("/echo")
    def echo() -> flask.Response:
        engine = get_request_obfuscation_engine()
        results.append(engine is None)
        return flask.jsonify({})

    client = app.test_client()
    resp = client.get("/echo")
    assert resp.status_code == 200

    assert len(results) == 1
    assert results[0] is True, "Engine should be None when base obfuscators are not configured"


def test_profiling_disabled_by_default() -> None:
    app = _create_test_app_no_obfuscation()
    profiling_seen: list[LogFormatProfilingContext | None] = []

    @app.route("/capture")
    def capture() -> flask.Response:
        profiling_seen.append(get_log_format_profiling())
        return flask.jsonify({})

    client = app.test_client()
    resp = client.get("/capture")
    assert resp.status_code == 200
    assert profiling_seen[0] is None


def test_profiling_not_emitted_when_count_is_zero(
    caplog: pytest.LogCaptureFixture,
) -> None:
    app = flask.Flask(__name__)
    dl_api_commons_flask_middlewares.ContextVarMiddleware().wrap_flask_app(app)
    setup_obfuscation_context_middleware(app, log_format_profiling_enabled=True)

    @app.route("/echo")
    def echo() -> flask.Response:
        return flask.jsonify({"status": "ok"})

    client = app.test_client()
    with caplog.at_level(logging.INFO, logger="dl_obfuscator.profiling"):
        resp = client.get("/echo")
    assert resp.status_code == 200

    profiling_records = [r for r in caplog.records if r.name == "dl_obfuscator.profiling"]
    assert len(profiling_records) == 0  # call_count stays 0 (no StdoutFormatter in test app)


def test_profiling_emitted_when_formatter_active(
    caplog: pytest.LogCaptureFixture,
) -> None:
    class _FormattingHandler(logging.Handler):
        def __init__(self) -> None:
            super().__init__()
            self.setFormatter(StdoutFormatter())

        def emit(self, record: logging.LogRecord) -> None:
            self.format(record)

    test_logger = logging.getLogger("test_profiling_formatter_flask")
    handler = _FormattingHandler()
    test_logger.addHandler(handler)
    test_logger.setLevel(logging.DEBUG)
    test_logger.propagate = False

    app = flask.Flask(__name__)
    dl_api_commons_flask_middlewares.ContextVarMiddleware().wrap_flask_app(app)
    setup_obfuscation_context_middleware(app, log_format_profiling_enabled=True)

    @app.route("/log")
    def log_route() -> flask.Response:
        test_logger.info("request handled")
        return flask.jsonify({"status": "ok"})

    client = app.test_client()
    try:
        with caplog.at_level(logging.INFO, logger="dl_obfuscator.profiling"):
            resp = client.get("/log")
        assert resp.status_code == 200
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


def _build_seeded_flask_app(
    secret_headers: CIMultiDict | None = None,
    auth_data: dl_auth.AuthData | None = None,
    set_auth_data: bool = False,
) -> flask.Flask:
    global_keeper = SecretKeeper()
    global_keeper.add_secret("GLOBAL_TOKEN", "global_token")

    app = flask.Flask(__name__)
    dl_api_commons_flask_middlewares.ContextVarMiddleware().wrap_flask_app(app)
    dl_api_commons_flask_middlewares.RequestLoggingContextControllerMiddleWare().set_up(app)
    dl_api_commons_flask_middlewares.RequestIDService(
        request_id_app_prefix=None,
        append_local_req_id=False,
    ).set_up(app)
    dl_api_commons_flask_middlewares.RCIHeadersMiddleware().set_up(app)

    def _seed_temp_rci() -> None:
        temp_rci = ReqCtxInfoMiddleware.get_temp_rci()
        clone_kwargs: dict = {}
        if secret_headers is not None:
            clone_kwargs["secret_headers"] = secret_headers
        if set_auth_data:
            clone_kwargs["auth_data"] = auth_data
        if clone_kwargs:
            ReqCtxInfoMiddleware.replace_temp_rci(temp_rci.clone(**clone_kwargs))

    # Register seeding BEFORE setup_obfuscation_context_middleware so it runs first.
    app.before_request(_seed_temp_rci)

    app.config[OBFUSCATION_BASE_OBFUSCATORS_KEY] = create_base_obfuscators(global_keeper=global_keeper)
    setup_obfuscation_context_middleware(app)

    dl_api_commons_flask_middlewares.ReqCtxInfoMiddleware().set_up(app)

    return app


class TestMiddlewareSecretPopulationFlask:
    def test_secret_headers_added_to_keeper(self) -> None:
        rci_secret_headers = CIMultiDict(
            [
                ("Authorization", "Bearer secret-bearer-token-XXXX"),
                ("Cookie", "Session_id=long-cookie-value-XXXX"),
            ]
        )
        app = _build_seeded_flask_app(secret_headers=rci_secret_headers)

        captured: list[dict[str, str]] = []

        @app.route("/secrets")
        def _secrets() -> flask.Response:
            rci = ReqCtxInfoMiddleware.get_request_context_info()
            captured.append(dict(rci.secret_keeper.secrets))
            return flask.jsonify({})

        client = app.test_client()
        resp = client.get("/secrets")
        assert resp.status_code == 200

        secrets = captured[0]
        assert secrets.get("Bearer secret-bearer-token-XXXX") == "header.Authorization"
        assert secrets.get("Session_id=long-cookie-value-XXXX") == "header.Cookie"

    def test_auth_data_added_to_keeper(self) -> None:
        auth_data = _AuthDataStub(oauth_token="oauth-token-value-XXXX", public_id="user-123")
        app = _build_seeded_flask_app(auth_data=auth_data, set_auth_data=True)

        captured: list[dict[str, str]] = []

        @app.route("/secrets")
        def _secrets() -> flask.Response:
            rci = ReqCtxInfoMiddleware.get_request_context_info()
            captured.append(dict(rci.secret_keeper.secrets))
            return flask.jsonify({})

        client = app.test_client()
        resp = client.get("/secrets")
        assert resp.status_code == 200

        secrets = captured[0]
        assert secrets.get("oauth-token-value-XXXX") == "auth_data.oauth_token"
        # `public_id` has default repr=True (not a secret marker) -> must NOT be in the keeper.
        assert "user-123" not in secrets

    def test_no_auth_data_no_crash(self) -> None:
        rci_secret_headers = CIMultiDict([("Authorization", "Bearer only-header-value-YYYY")])
        app = _build_seeded_flask_app(
            secret_headers=rci_secret_headers,
            auth_data=None,
            set_auth_data=True,
        )

        captured: list[dict[str, str]] = []

        @app.route("/secrets")
        def _secrets() -> flask.Response:
            rci = ReqCtxInfoMiddleware.get_request_context_info()
            captured.append(dict(rci.secret_keeper.secrets))
            return flask.jsonify({})

        client = app.test_client()
        resp = client.get("/secrets")
        assert resp.status_code == 200

        secrets = captured[0]
        # No exception thrown; header still made it in, no auth_data.* keys present.
        assert secrets.get("Bearer only-header-value-YYYY") == "header.Authorization"
        assert not any(name.startswith("auth_data.") for name in secrets.values())
