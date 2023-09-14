import logging
from http import HTTPStatus
from typing import Any, Optional

import pytest
from aiohttp import web

from bi_cloud_integration.iam_mock import IAMServicesMockFacade
from bi_api_commons_ya_cloud.constants import YcTokenHeaderMode
from bi_api_commons.aio.middlewares.commit_rci import commit_rci_middleware
from bi_api_commons.aio.middlewares.error_handling_outer import AIOHTTPErrorHandler, ErrorData, ErrorLevel
from bi_api_commons.aio.middlewares.request_bootstrap import RequestBootstrap
from bi_api_commons.aio.middlewares.request_id import RequestId
from bi_api_commons.aiohttp.aiohttp_wrappers import DLRequestBase
from bi_api_commons_ya_cloud.aio.middlewares.yc_auth import YCAuthService
from bi_api_commons_ya_cloud.models import IAMAuthData
from bi_api_commons_ya_cloud.yc_access_control_model import (
    AuthorizationMode,
    AuthorizationModeDataCloud,
    AuthorizationModeYandexCloud,
)

from bi_api_lib_testing.client import TestClientConverterAiohttpToFlask

from ..test_yc_auth_scenarios import (
    Scenario_YCAuth_ModeYC_DenyCookieAuth,
    Scenario_YCAuth_ModeDataCloud_DenyCookieAuth,
    Scenario_YCAuth_ModeYC_AllowCookieAuth,
)


LOGGER = logging.getLogger(__name__)


def create_yc_auth_enabled_app(
        authorization_mode: AuthorizationMode,
        iam_mock: IAMServicesMockFacade,
        yc_token_header_mode: YcTokenHeaderMode,
        skip_auth_exact_path: str,
        skip_auth_prefixed_path: str,
        dummy_tenant_id: Optional[str] = None,
        enable_cookie_auth: bool = False,
) -> web.Application:
    async def root(request: web.Request) -> web.StreamResponse:
        dl_request = DLRequestBase.get_for_request(request)
        rci = dl_request.rci
        auth_data = rci.auth_data
        assert isinstance(auth_data, IAMAuthData)
        response = dict(
            user_id=rci.user_id,
            iam_token=auth_data.iam_token,
        )
        tenant_id = rci.tenant.get_tenant_id() if rci.tenant is not None else None
        if dummy_tenant_id is None or dummy_tenant_id is not None and tenant_id != dummy_tenant_id:
            response.update(tenant_id=tenant_id)

        return web.json_response(response)

    async def no_auth_by_skip_list(_: web.Request) -> web.StreamResponse:
        return web.json_response({})

    class DummyErrorHandler(AIOHTTPErrorHandler):
        def _classify_error(self, err: Exception, request: web.Request) -> ErrorData:
            if isinstance(err, web.HTTPException):
                return ErrorData(
                    status_code=err.status,
                    http_reason=err.reason,
                    response_body=dict(message=err.reason),
                    level=ErrorLevel.info,
                )
            else:
                LOGGER.exception(err)
                return ErrorData(
                    status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
                    http_reason="Internal server error",
                    response_body=dict(message=str(err)),
                    level=ErrorLevel.error,
                )

    req_id_service = RequestId()
    app = web.Application(middlewares=[
        RequestBootstrap(
            req_id_service=req_id_service,
            error_handler=DummyErrorHandler(sentry_app_name_tag=None),
        ).middleware,
        YCAuthService(
            authorization_mode=authorization_mode,
            enable_cookie_auth=enable_cookie_auth,
            access_service_cfg=iam_mock.service_config,
            session_service_cfg=iam_mock.service_config,
            allowed_folder_ids=None,
            yc_token_header_mode=yc_token_header_mode,
            skip_path_list=(
                skip_auth_exact_path,
                skip_auth_prefixed_path,
            ),
        ).middleware,
        commit_rci_middleware(),
    ])
    app.on_response_prepare.append(req_id_service.on_response_prepare)
    app.router.add_get("/auth_ctx", root)
    app.router.add_get(skip_auth_exact_path, no_auth_by_skip_list)

    return app


class Test_AIOHTTP_YCAuth_ModeDataCloud_DenyCookieAuth(Scenario_YCAuth_ModeDataCloud_DenyCookieAuth):
    @pytest.fixture()
    async def client(
            self,
            iam,
            yc_token_header_mode,
            aiohttp_client,
            project_required_permission,
            skip_auth_exact_path,
            skip_auth_prefixed_path,
            project_id,
            loop,
    ) -> Any:
        app = create_yc_auth_enabled_app(
            authorization_mode=AuthorizationModeDataCloud(
                project_permission_to_check=project_required_permission,
            ),
            iam_mock=iam,
            yc_token_header_mode=yc_token_header_mode,
            skip_auth_exact_path=skip_auth_exact_path,
            skip_auth_prefixed_path=skip_auth_prefixed_path,
            dummy_tenant_id=project_id,
        )
        return TestClientConverterAiohttpToFlask(loop=loop, aio_client=(await aiohttp_client(app)))


class Test_AIOHTTP_YCAuth_ModeYC_DenyCookieAuth(Scenario_YCAuth_ModeYC_DenyCookieAuth):
    @pytest.fixture()
    async def client(
            self,
            iam,
            yc_token_header_mode,
            aiohttp_client,
            folder_required_permission,
            skip_auth_exact_path,
            skip_auth_prefixed_path,
            loop,
            organization_required_permission,
    ) -> Any:
        app = create_yc_auth_enabled_app(
            authorization_mode=AuthorizationModeYandexCloud(
                folder_permission_to_check=folder_required_permission,
                organization_permission_to_check=organization_required_permission,
            ),
            iam_mock=iam,
            yc_token_header_mode=yc_token_header_mode,
            skip_auth_exact_path=skip_auth_exact_path,
            skip_auth_prefixed_path=skip_auth_prefixed_path,
        )
        return TestClientConverterAiohttpToFlask(loop=loop, aio_client=(await aiohttp_client(app)))


class Test_AIOHTTP_YCAuth_ModeYC_AllowCookieAuth(Scenario_YCAuth_ModeYC_AllowCookieAuth):
    @pytest.fixture()
    async def client(
            self,
            iam,
            yc_token_header_mode,
            aiohttp_client,
            folder_required_permission,
            skip_auth_exact_path,
            skip_auth_prefixed_path,
            loop,
            organization_required_permission,
    ) -> Any:
        app = create_yc_auth_enabled_app(
            authorization_mode=AuthorizationModeYandexCloud(
                folder_permission_to_check=folder_required_permission,
                organization_permission_to_check=organization_required_permission,
            ),
            iam_mock=iam,
            yc_token_header_mode=yc_token_header_mode,
            skip_auth_exact_path=skip_auth_exact_path,
            skip_auth_prefixed_path=skip_auth_prefixed_path,
            enable_cookie_auth=True,
        )
        return TestClientConverterAiohttpToFlask(loop=loop, aio_client=(await aiohttp_client(app)))
