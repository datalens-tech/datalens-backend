import flask
import pytest

from dl_api_commons.base_models import RequestContextInfo
from dl_api_commons.exc import FlaskRCINotSet
import dl_api_commons.flask.middlewares as dl_api_commons_flask_middlewares


@pytest.mark.parametrize("rci_commit", [True, False])
def test_rci_headers_populates_rci(rci_commit: bool) -> None:
    app = flask.Flask(__name__)

    dl_api_commons_flask_middlewares.ContextVarMiddleware().wrap_flask_app(app)
    dl_api_commons_flask_middlewares.RequestLoggingContextControllerMiddleWare().set_up(app)
    dl_api_commons_flask_middlewares.RequestIDService(
        request_id_app_prefix=None,
        append_local_req_id=False,
    ).set_up(app)
    dl_api_commons_flask_middlewares.RCIHeadersMiddleware(
        plain_headers=("X-Custom-Plain",),
        secret_headers=("X-Custom-Secret",),
    ).set_up(app)
    if rci_commit:
        dl_api_commons_flask_middlewares.ReqCtxInfoMiddleware().set_up(app)

    rci_list: list[RequestContextInfo] = []

    @app.route("/test")
    def test_view() -> flask.Response:
        if rci_commit:
            assert dl_api_commons_flask_middlewares.ReqCtxInfoMiddleware.is_rci_committed()
            rci_list.append(dl_api_commons_flask_middlewares.ReqCtxInfoMiddleware.get_request_context_info())
        else:
            assert not dl_api_commons_flask_middlewares.ReqCtxInfoMiddleware.is_rci_committed()
            with pytest.raises(FlaskRCINotSet):
                dl_api_commons_flask_middlewares.ReqCtxInfoMiddleware.get_request_context_info()
            rci_list.append(dl_api_commons_flask_middlewares.ReqCtxInfoMiddleware.get_temp_rci())
        return flask.jsonify({})

    client = app.test_client()
    resp = client.get(
        "/test",
        headers={
            "X-Request-ID": "tests-req-id",
            "X-Custom-Plain": "hello",
            "X-Custom-secret": "secret",
            "X-Ignore-Me": "nope",
            "referer": "http://example.com",
        },
    )
    assert resp.status_code == 200
    assert len(rci_list) == 1
    rci = rci_list[0]
    assert rci.request_id == "tests-req-id"
    assert rci.plain_headers.get("X-Custom-Plain") == "hello"
    assert rci.plain_headers.get("referer") == "http://example.com"
    assert rci.plain_headers.get("X-Ignore-Me") is None
    assert rci.secret_headers.get("X-Custom-Secret") == "secret"
