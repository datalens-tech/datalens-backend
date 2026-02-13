import flask

from dl_api_commons.base_models import RequestContextInfo
import dl_api_commons.flask.middlewares as dl_api_commons_flask_middlewares


def test_rci_headers_populates_temp_rci() -> None:
    app = flask.Flask(__name__)

    dl_api_commons_flask_middlewares.ContextVarMiddleware().wrap_flask_app(app)
    dl_api_commons_flask_middlewares.RequestLoggingContextControllerMiddleWare().set_up(app)
    dl_api_commons_flask_middlewares.RequestIDService(
        request_id_app_prefix=None,
        append_local_req_id=False,
    ).set_up(app)
    dl_api_commons_flask_middlewares.RCIHeadersMiddleware(
        plain_headers=("x-custom-plain",),
    ).set_up(app)

    temp_rci_list: list[RequestContextInfo] = []

    @app.route("/test")
    def test_view() -> flask.Response:
        assert not dl_api_commons_flask_middlewares.ReqCtxInfoMiddleware.is_rci_committed()
        temp_rci_list.append(dl_api_commons_flask_middlewares.ReqCtxInfoMiddleware.get_temp_rci())
        return flask.jsonify({})

    client = app.test_client()
    resp = client.get(
        "/test",
        headers={
            "x-request-id": "req123",
            "x-custom-plain": "hello",
            "x-should-ignore": "nope",
            "referer": "http://example.com",
        },
    )
    assert resp.status_code == 200
    assert len(temp_rci_list) == 1
    rci = temp_rci_list[0]
    assert rci.plain_headers.get("x-custom-plain") == "hello"
    assert rci.plain_headers.get("referer") == "http://example.com"
    assert rci.plain_headers.get("x-should-ignore") is None


def test_rci_headers_with_commit_rci() -> None:
    app = flask.Flask(__name__)

    dl_api_commons_flask_middlewares.ContextVarMiddleware().wrap_flask_app(app)
    dl_api_commons_flask_middlewares.RequestLoggingContextControllerMiddleWare().set_up(app)
    dl_api_commons_flask_middlewares.RequestIDService(
        request_id_app_prefix=None,
        append_local_req_id=False,
    ).set_up(app)
    dl_api_commons_flask_middlewares.RCIHeadersMiddleware(
        plain_headers=("x-custom-plain",),
    ).set_up(app)
    dl_api_commons_flask_middlewares.ReqCtxInfoMiddleware(
        plain_headers=("x-custom-plain",),
    ).set_up(app)

    rci_list: list[RequestContextInfo] = []

    @app.route("/test")
    def test_view() -> flask.Response:
        rci_list.append(dl_api_commons_flask_middlewares.ReqCtxInfoMiddleware.get_request_context_info())
        return flask.jsonify({})

    client = app.test_client()
    resp = client.get(
        "/test",
        headers={
            "x-request-id": "req456",
            "x-custom-plain": "custom_value",
            "referer": "http://example.com",
            "x-should-ignore": "nope",
        },
    )
    assert resp.status_code == 200
    assert len(rci_list) == 1
    rci = rci_list[0]
    assert rci.request_id == "req456"
    assert rci.plain_headers.get("x-custom-plain") == "custom_value"
    assert rci.plain_headers.get("referer") == "http://example.com"
    assert rci.plain_headers.get("x-should-ignore") is None
