import flask

import dl_api_commons.flask.middlewares as dl_api_commons_flask_middlewares


def test_rci_is_committed() -> None:
    app = flask.Flask(__name__)

    dl_api_commons_flask_middlewares.ContextVarMiddleware().wrap_flask_app(app)
    dl_api_commons_flask_middlewares.RequestLoggingContextControllerMiddleWare().set_up(app)
    dl_api_commons_flask_middlewares.RequestIDService(
        request_id_app_prefix=None,
        append_local_req_id=False,
    ).set_up(app)
    dl_api_commons_flask_middlewares.RCIHeadersMiddleware().set_up(app)
    dl_api_commons_flask_middlewares.ReqCtxInfoMiddleware().set_up(app)

    rci_lst = []

    @app.route("/test")
    def test_connection() -> flask.Response:
        assert dl_api_commons_flask_middlewares.ReqCtxInfoMiddleware.is_rci_committed()
        rci_lst.append(dl_api_commons_flask_middlewares.ReqCtxInfoMiddleware.get_request_context_info())
        return flask.jsonify({})

    client = app.test_client()

    resp = client.get("/test")
    assert resp.status_code == 200
    assert len(rci_lst) == 1
