from __future__ import annotations

from typing import Any

import flask
import pytest

from bi_cloud_integration.iam_mock import IAMServicesMockFacade
from bi_constants.api_constants import YcTokenHeaderMode
from bi_api_commons.yc_access_control_model import (
    AuthorizationMode,
    AuthorizationModeYandexCloud,
    AuthorizationModeDataCloud,
)
from bi_api_commons.base_models import IAMAuthData
from bi_api_commons.flask.middlewares.commit_rci_middleware import ReqCtxInfoMiddleware
from bi_api_commons.flask.middlewares.context_var_middleware import ContextVarMiddleware
from bi_api_commons.flask.middlewares.logging_context import RequestLoggingContextControllerMiddleWare
from bi_api_commons.flask.middlewares.request_id import RequestIDService
from bi_api_commons_ya_cloud.flask.middlewares.yc_auth import FlaskYCAuthService

from ..test_yc_auth_scenarios import (
    Scenario_YCAuth_ModeYC_AllowCookieAuth,
    Scenario_YCAuth_ModeYC_DenyCookieAuth,
    Scenario_YCAuth_ModeDataCloud_DenyCookieAuth,
)


def create_yc_auth_enabled_app(
        authorization_mode: AuthorizationMode,
        enable_cookie_auth: bool,
        iam_mock: IAMServicesMockFacade,
        yc_token_header_mode: YcTokenHeaderMode,
        skip_auth_exact_path: str,
        skip_auth_prefixed_path: str,
) -> flask.Flask:
    app = flask.Flask(__name__)
    ContextVarMiddleware().wrap_flask_app(app)
    RequestLoggingContextControllerMiddleWare().set_up(app)
    RequestIDService(
        append_local_req_id=False,
        request_id_app_prefix=None,
    ).set_up(app)

    FlaskYCAuthService(
        enable_cookie_auth=enable_cookie_auth,
        authorization_mode=authorization_mode,
        access_service_cfg=iam_mock.service_config,
        session_service_cfg=iam_mock.service_config,
        yc_token_header_mode=yc_token_header_mode,
        # We have no VM-metadata service here
        static_sa_token_for_session_service="dummy-token",
        skip_path_list=(
            skip_auth_exact_path,
            skip_auth_prefixed_path,
        ),
    ).set_up(app)

    ReqCtxInfoMiddleware().set_up(app)

    @app.route("/auth_ctx")
    def test_view():
        rci = ReqCtxInfoMiddleware.get_request_context_info()
        auth_data = rci.auth_data
        assert isinstance(auth_data, IAMAuthData)

        response = dict(
            user_id=rci.user_id,
            iam_token=auth_data.iam_token,
        )
        tenant_id = rci.get_tenant_id_if_cloud_env_none_else()
        if tenant_id is not None:
            response.update(tenant_id=tenant_id)

        return flask.jsonify(response)

    @app.route(skip_auth_exact_path)
    def skip_auth():
        return flask.jsonify({})

    return app


class Test_Flask_YCAuth_ModeDataCloud_DenyCookieAuth(Scenario_YCAuth_ModeDataCloud_DenyCookieAuth):
    @pytest.fixture()
    def client(
        self, iam, yc_token_header_mode, project_required_permission,
        skip_auth_exact_path, skip_auth_prefixed_path
    ) -> Any:
        app = create_yc_auth_enabled_app(
            authorization_mode=AuthorizationModeDataCloud(
                project_permission_to_check=project_required_permission
            ),
            enable_cookie_auth=False,
            iam_mock=iam,
            yc_token_header_mode=yc_token_header_mode,
            skip_auth_exact_path=skip_auth_exact_path,
            skip_auth_prefixed_path=skip_auth_prefixed_path,
        )
        return app.test_client()


class Test_Flask_YCAuth_ModeYC_DenyCookieAuth(Scenario_YCAuth_ModeYC_DenyCookieAuth):
    @pytest.fixture()
    def client(
            self,
            folder_required_permission,
            iam,
            yc_token_header_mode,
            skip_auth_exact_path,
            skip_auth_prefixed_path,
            organization_required_permission,
    ) -> Any:
        app = create_yc_auth_enabled_app(
            authorization_mode=AuthorizationModeYandexCloud(
                folder_permission_to_check=folder_required_permission,
                organization_permission_to_check=organization_required_permission,
            ),
            enable_cookie_auth=False,
            iam_mock=iam,
            yc_token_header_mode=yc_token_header_mode,
            skip_auth_exact_path=skip_auth_exact_path,
            skip_auth_prefixed_path=skip_auth_prefixed_path,
        )
        return app.test_client()


class Test_Flask_YCAuth_ModeYC_AllowCookieAuth(Scenario_YCAuth_ModeYC_AllowCookieAuth):
    @pytest.fixture()
    def client(
            self,
            folder_required_permission,
            iam,
            yc_token_header_mode,
            skip_auth_exact_path,
            skip_auth_prefixed_path,
            organization_required_permission,
    ) -> Any:
        app = create_yc_auth_enabled_app(
            authorization_mode=AuthorizationModeYandexCloud(
                folder_permission_to_check=folder_required_permission,
                organization_permission_to_check=organization_required_permission,
            ),
            enable_cookie_auth=True,
            iam_mock=iam,
            yc_token_header_mode=yc_token_header_mode,
            skip_auth_exact_path=skip_auth_exact_path,
            skip_auth_prefixed_path=skip_auth_prefixed_path,
        )
        return app.test_client()
