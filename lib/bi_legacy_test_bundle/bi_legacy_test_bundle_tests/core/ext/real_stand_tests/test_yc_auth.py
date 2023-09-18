# TODO: move to bi_api_commons_ya_cloud

from __future__ import annotations

from http import HTTPStatus
from typing import Callable, Awaitable, Sequence, Dict

import attr
import pytest
import shortuuid
from aiohttp import web
from aiohttp.test_utils import TestClient
from aiohttp.typedefs import Handler

from dl_api_commons.exc import InvalidHeaderException
from dl_testing.utils import skip_outside_devhost
from dl_constants.api_constants import DLHeadersCommon
from bi_api_commons_ya_cloud.constants import YcTokenHeaderMode, DLHeadersYC

from dl_api_commons.aio.middlewares.error_handling_outer import AIOHTTPErrorHandler, ErrorData, ErrorLevel
from dl_api_commons.aio.middlewares.request_bootstrap import RequestBootstrap
from dl_api_commons.aio.middlewares.request_id import RequestId
from bi_api_commons_ya_cloud.aio.middlewares.yc_auth import YCAuthService
from bi_api_commons_ya_cloud.yc_access_control_model import AuthorizationModeYandexCloud
from bi_api_commons_ya_cloud.yc_auth import make_default_yc_auth_service_config


@attr.s
class _AppConfig:
    yc_token_header_mode: YcTokenHeaderMode = attr.ib(default=YcTokenHeaderMode.INTERNAL)
    extra_mw: Sequence = attr.ib(default=())
    extra_handlers: Dict[str, Handler] = attr.ib(factory=dict)


_AppFactory = Callable[[_AppConfig], Awaitable[TestClient]]


@pytest.fixture(scope='function')
@pytest.mark.usefixtures('loop')
def app_factory(aiohttp_client, bi_common_stand_access_service_endpoint) -> _AppFactory:
    async def f(config: _AppConfig) -> TestClient:
        async def root(_):
            return web.json_response({})

        class ErrorHandler(AIOHTTPErrorHandler):
            def _classify_error(self, err: Exception, request: web.Request) -> ErrorData:
                if isinstance(err, InvalidHeaderException):
                    return ErrorData(
                        status_code=HTTPStatus.BAD_REQUEST,
                        response_body=dict(message=str(err), header_name=err.header_name),
                        level=ErrorLevel.info,
                    )
                elif isinstance(err, web.HTTPException):
                    return ErrorData(
                        status_code=err.status,
                        http_reason=err.reason,
                        response_body=dict(message=err.reason),
                        level=ErrorLevel.info,
                    )
                else:
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
                error_handler=ErrorHandler(sentry_app_name_tag=None),
            ).middleware,
            YCAuthService(
                authorization_mode=AuthorizationModeYandexCloud(
                    folder_permission_to_check='datalens.instances.use',
                    organization_permission_to_check='datalens.instances.use',
                ),
                enable_cookie_auth=False,
                access_service_cfg=make_default_yc_auth_service_config(
                    endpoint=bi_common_stand_access_service_endpoint,
                    total_timeout=22,
                    call_timeout=5,
                ),
                allowed_folder_ids=None,
                yc_token_header_mode=config.yc_token_header_mode,
            ).middleware,
            *config.extra_mw,
        ])
        app.on_response_prepare.append(req_id_service.on_response_prepare)
        app.router.add_get("/", root)
        for route, extra_handler in config.extra_handlers.items():
            app.router.add_route("*", route, extra_handler)
        return await aiohttp_client(app)

    yield f


@skip_outside_devhost
@pytest.mark.asyncio
@pytest.mark.parametrize("hdr_mode,hdr_name,hdr_val_format", [
    (YcTokenHeaderMode.UNIVERSAL, DLHeadersYC.IAM_TOKEN.value, "{}"),
    (YcTokenHeaderMode.UNIVERSAL, DLHeadersCommon.AUTHORIZATION_TOKEN.value, "Bearer {}"),
    (YcTokenHeaderMode.EXTERNAL, DLHeadersCommon.AUTHORIZATION_TOKEN.value, "Bearer {}"),
    (YcTokenHeaderMode.INTERNAL, DLHeadersYC.IAM_TOKEN.value, "{}"),
])
async def test_auth_ok_universal(
        bi_common_stand_folder_id,
        app_factory,
        hdr_mode, hdr_name, hdr_val_format,
        bi_common_stand_ok_user_account_creds,
):
    folder_id = bi_common_stand_folder_id
    test_user_creds = bi_common_stand_ok_user_account_creds

    app = await app_factory(_AppConfig(yc_token_header_mode=hdr_mode))
    resp_cm = app.get(
        "/",
        headers={
            hdr_name: hdr_val_format.format(test_user_creds.token),
            DLHeadersYC.FOLDER_ID.value: folder_id,
        })
    async with resp_cm as resp:
        rd = await resp.json()
        assert resp.status == 200, rd

    resp_cm = app.get(
        "/",
        headers={
            hdr_name: hdr_val_format.format(shortuuid.uuid()),
            DLHeadersYC.FOLDER_ID.value: folder_id,
        })
    async with resp_cm as resp:
        rd = await resp.json()
        assert resp.status == 401, ("Should return 401 on invalid IAM token", rd)


@pytest.mark.asyncio
@pytest.mark.parametrize('yc_token_mode,hdr_name_in_msg', [
    (YcTokenHeaderMode.UNIVERSAL, DLHeadersCommon.AUTHORIZATION_TOKEN.value),
    (YcTokenHeaderMode.EXTERNAL, DLHeadersCommon.AUTHORIZATION_TOKEN.value),
    (YcTokenHeaderMode.INTERNAL, DLHeadersYC.IAM_TOKEN.value),
])
async def test_missing_header_universal(app_factory: _AppFactory, yc_token_mode, hdr_name_in_msg):
    app = await app_factory(_AppConfig(yc_token_header_mode=yc_token_mode))

    async with app.get("/") as resp:
        rd = await resp.json()
        assert resp.status == 401, rd
        assert rd == {
            # 'header_name': hdr_name_in_msg,
            # 'message': 'Header required, but missing',
            'message': 'No authentication data provided',
        }
