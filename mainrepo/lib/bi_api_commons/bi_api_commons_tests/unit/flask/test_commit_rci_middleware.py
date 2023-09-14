from __future__ import annotations

import flask

from bi_api_commons.base_models import RequestContextInfo
from bi_api_commons.flask.middlewares.commit_rci_middleware import ReqCtxInfoMiddleware
from bi_api_commons.flask.middlewares.context_var_middleware import ContextVarMiddleware
from bi_api_commons.flask.middlewares.logging_context import RequestLoggingContextControllerMiddleWare
from bi_api_commons.flask.middlewares.request_id import RequestIDService


def test_integration(caplog):
    caplog.set_level("DEBUG")
    caplog.clear()
    app = flask.Flask(__name__)

    ContextVarMiddleware().wrap_flask_app(app)

    RequestLoggingContextControllerMiddleWare().set_up(app)
    RequestIDService(
        request_id_app_prefix=None,
        append_local_req_id=False,
    ).set_up(app)
    ReqCtxInfoMiddleware(
        plain_headers=("include-me",),
    ).set_up(app)

    rci_lst = []

    @app.route("/test")
    def test_connection():
        rci_lst.append(ReqCtxInfoMiddleware.get_request_context_info())
        return flask.jsonify({})

    client = app.test_client()

    resp = client.get(
        "/test",
        headers={
            "x-request-id": "asdf1234",
            "include-me": "include_me",
            "exclude-me": "exclude_me",
            "referer": "referer",
            "x-chart-id": "x-chart-id",
        },
    )
    assert resp.status_code == 200
    assert len(rci_lst) == 1
    rci: RequestContextInfo = rci_lst[0]

    assert rci.request_id == "asdf1234"
    assert rci.plain_headers.get("include-me") == "include_me"
    assert rci.plain_headers.get("exclude-me") is None
    # TODO FIX: Check headers
