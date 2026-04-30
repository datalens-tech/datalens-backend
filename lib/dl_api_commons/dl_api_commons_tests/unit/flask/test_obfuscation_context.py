import logging

import flask
import pytest

import dl_api_commons.flask.middlewares as dl_api_commons_flask_middlewares
from dl_api_commons.flask.middlewares.obfuscation_context import setup_obfuscation_context_middleware
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
