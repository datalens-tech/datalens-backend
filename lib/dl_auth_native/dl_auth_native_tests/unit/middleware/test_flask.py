import datetime
import unittest.mock as mock

import flask
import pytest

import dl_api_commons.flask.middlewares as dl_api_commons_flask_middlewares
import dl_auth_native


@pytest.fixture(name="flask_app")
def fixture_flask_app(
    token_decoder: dl_auth_native.DecoderProtocol,
) -> flask.Flask:
    app = flask.Flask(__name__)

    dl_api_commons_flask_middlewares.ContextVarMiddleware().wrap_flask_app(app)
    dl_api_commons_flask_middlewares.RequestLoggingContextControllerMiddleWare().set_up(app)
    dl_api_commons_flask_middlewares.RequestIDService(
        append_local_req_id=False,
        request_id_app_prefix=None,
    ).set_up(app)
    dl_auth_native.FlaskMiddleware(
        token_decoder=token_decoder,
    ).set_up(app)

    @app.route("/")
    def handler() -> flask.Response:
        temp_rci = getattr(flask.g, "_bi_temp_request_context_info", None)
        assert isinstance(temp_rci, dl_api_commons_flask_middlewares.RequestContextInfo)
        assert temp_rci.user_id == "test_user_id"
        return flask.jsonify({})

    return app


def test_default(
    flask_app: flask.Flask,
    token_decoder: mock.Mock,
) -> None:
    token_decoder.decode.return_value = dl_auth_native.Payload(
        user_id="test_user_id",
        expires_at=datetime.datetime.now(),
    )

    with flask_app.test_client() as client:
        response = client.get("/", headers={"Authorization": "Bearer token"})

    assert response.status_code == 200
    assert token_decoder.decode.called_once_with("token")


def test_missing_token_header(
    flask_app: flask.Flask,
) -> None:
    with flask_app.test_client() as client:
        response = client.get("/")

    assert response.status_code == 401
    assert "User access token header is missing" in response.text


def test_bad_token_type(
    flask_app: flask.Flask,
) -> None:
    with flask_app.test_client() as client:
        response = client.get("/", headers={"Authorization": "Basic token"})

    assert response.status_code == 401
    assert "Bad token type" in response.text


def test_invalid_token(
    flask_app: flask.Flask,
    token_decoder: mock.Mock,
) -> None:
    token_decoder.decode.side_effect = dl_auth_native.DecodeError("invalid token")

    with flask_app.test_client() as client:
        response = client.get("/", headers={"Authorization": "Bearer token"})

    assert response.status_code == 401
    assert "Invalid user access token: invalid token" in response.text
