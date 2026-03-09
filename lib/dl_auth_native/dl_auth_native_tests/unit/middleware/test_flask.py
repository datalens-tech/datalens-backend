import datetime
import unittest.mock as mock

import flask
from flask.views import MethodView
import pytest

import dl_api_commons.flask.middlewares as dl_api_commons_flask_middlewares
from dl_api_commons.flask.middlewares.commit_rci_middleware import ReqCtxInfoMiddleware
from dl_api_commons.flask.required_resources import RequiredResourceCommon
import dl_auth_native
from dl_constants.api_constants import DLHeadersCommon


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


MASTER_TOKEN = "test-master-token-secret"


@pytest.fixture(name="flask_app_with_service_auth")
def fixture_flask_app_with_service_auth(
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
        master_token=MASTER_TOKEN,
    ).set_up(app)
    ReqCtxInfoMiddleware().set_up(app)

    class ServiceView(MethodView):
        REQUIRED_RESOURCES = frozenset({RequiredResourceCommon.ONLY_SERVICES_ALLOWED})

        def get(self) -> flask.Response:
            return flask.jsonify({"ok": True})

    app.add_url_rule("/service", view_func=ServiceView.as_view("service"))

    @app.route("/user")
    def user_handler() -> flask.Response:
        return flask.jsonify({"ok": True})

    return app


def test_service_auth_correct_token(
    flask_app_with_service_auth: flask.Flask,
) -> None:
    with flask_app_with_service_auth.test_client() as client:
        response = client.get(
            "/service",
            headers={DLHeadersCommon.US_MASTER_TOKEN.value: MASTER_TOKEN},
        )
    assert response.status_code == 200


def test_service_auth_wrong_token(
    flask_app_with_service_auth: flask.Flask,
) -> None:
    with flask_app_with_service_auth.test_client() as client:
        response = client.get(
            "/service",
            headers={DLHeadersCommon.US_MASTER_TOKEN.value: "wrong-token"},
        )
    assert response.status_code == 403
    assert "Invalid service token" in response.text


def test_service_auth_missing_token(
    flask_app_with_service_auth: flask.Flask,
) -> None:
    with flask_app_with_service_auth.test_client() as client:
        response = client.get("/service")
    assert response.status_code == 401
    assert "Service token header is missing" in response.text


def test_service_auth_not_configured(
    token_decoder: dl_auth_native.DecoderProtocol,
) -> None:
    """When master_token is None, service endpoints should return 401."""
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
    ReqCtxInfoMiddleware().set_up(app)

    class ServiceView(MethodView):
        REQUIRED_RESOURCES = frozenset({RequiredResourceCommon.ONLY_SERVICES_ALLOWED})

        def get(self) -> flask.Response:
            return flask.jsonify({"ok": True})

    app.add_url_rule("/service", view_func=ServiceView.as_view("service"))

    with app.test_client() as client:
        response = client.get(
            "/service",
            headers={DLHeadersCommon.US_MASTER_TOKEN.value: "some-token"},
        )
    assert response.status_code == 401
    assert "Service auth is not configured" in response.text


def test_user_endpoint_still_uses_jwt(
    flask_app_with_service_auth: flask.Flask,
    token_decoder: mock.Mock,
) -> None:
    """Regular endpoints should still require JWT auth, not accept master token."""
    with flask_app_with_service_auth.test_client() as client:
        response = client.get(
            "/user",
            headers={DLHeadersCommon.US_MASTER_TOKEN.value: MASTER_TOKEN},
        )
    assert response.status_code == 401
    assert "User access token header is missing" in response.text
