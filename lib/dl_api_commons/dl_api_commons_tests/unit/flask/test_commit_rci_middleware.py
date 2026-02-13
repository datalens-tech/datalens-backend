import flask
import pytest

from dl_api_commons.base_models import RequestContextInfo
import dl_api_commons.flask.middlewares as dl_api_commons_flask_middlewares


def test_integration(caplog: pytest.LogCaptureFixture) -> None:
    # TODO BI-7021 modify this test to actually check commit_rci after removing header population from commit_rci_middleware
    caplog.set_level("DEBUG")
    caplog.clear()
    app = flask.Flask(__name__)

    dl_api_commons_flask_middlewares.ContextVarMiddleware().wrap_flask_app(app)

    dl_api_commons_flask_middlewares.RequestLoggingContextControllerMiddleWare().set_up(app)
    dl_api_commons_flask_middlewares.RequestIDService(
        request_id_app_prefix=None,
        append_local_req_id=False,
    ).set_up(app)
    dl_api_commons_flask_middlewares.RCIHeadersMiddleware().set_up(app)
    dl_api_commons_flask_middlewares.ReqCtxInfoMiddleware(
        plain_headers=("include-me",),
    ).set_up(app)

    rci_lst = []

    @app.route("/test")
    def test_connection() -> flask.Response:
        rci_lst.append(dl_api_commons_flask_middlewares.ReqCtxInfoMiddleware.get_request_context_info())
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
