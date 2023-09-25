from __future__ import annotations

import flask

from dl_api_commons.flask.middlewares.context_var_middleware import ContextVarMiddleware
from dl_api_commons.flask.middlewares.tracing import TracingMiddleware


def test_app(caplog, loop):
    """Just to check that middleware doesn't break something"""
    caplog.set_level("DEBUG")

    app = flask.Flask(__name__)
    TracingMiddleware(
        url_prefix_exclude=(),
    ).wrap_flask_app(app)
    ContextVarMiddleware().wrap_flask_app(app)

    @app.route("/ok")
    def ok():
        return flask.jsonify({})

    @app.route("/err")
    def err():
        return 500, flask.jsonify({})

    client = app.test_client()

    resp = client.get("/ok")
    assert resp.status_code == 200

    resp = client.get("/err")
    assert resp.status_code == 500
