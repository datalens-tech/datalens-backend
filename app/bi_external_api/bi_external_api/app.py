import logging
import os
from typing import Sequence, Any, Type

from aiohttp import web

from bi_api_commons.aio.middlewares.commit_rci import commit_rci_middleware
from bi_api_commons.aio.middlewares.request_bootstrap import RequestBootstrap
from bi_api_commons.aio.middlewares.request_id import RequestId
from bi_api_commons_ya_team.aio.middlewares.blackbox_auth import blackbox_auth_middleware
from bi_api_commons_ya_cloud.aio.middlewares.yc_auth import YCAuthService
from bi_api_commons_ya_cloud.yc_access_control_model import AuthorizationModeDataCloud
from bi_api_commons_ya_cloud.yc_auth import make_default_yc_auth_service_config

from bi_configs.enums import AppType
from bi_configs.env_var_definitions import use_jaeger_tracer, jaeger_service_name_env_aware
from bi_configs.settings_loaders.fallback_cfg_resolver import YEnvFallbackConfigResolver
from bi_configs.settings_loaders.loader_env import load_settings_from_env_with_fallback_legacy
from bi_constants.api_constants import YcTokenHeaderMode

from bi_core.aio.middlewares.auth_trust_middleware import auth_trust_middleware
from bi_core.aio.middlewares.tracing import TracingService
from bi_core.aio.ping_view import PingView
from bi_core.logging_config import configure_logging
from bi_defaults.environments import InstallationsMap, EnvAliasesMap

from bi_external_api.aiohttp_services.base import ExtAPIRequest, AppConfig
from bi_external_api.aiohttp_services.error_handler import ExternalAPIErrorHandler
from bi_external_api.aiohttp_services.internal_api_clients_middleware import InternalAPIClientsMiddleware
from bi_external_api.enums import ExtAPIType
from bi_external_api.http_views.workbook_rest import WorkbookRestInstanceView
from bi_external_api.http_views.workbook_rpc import WorkbookRPCView, WorkbookRPCViewPrivate, WorkbookRPCViewDoubleCloud
from bi_external_api.http_views.workbook_rpc_unified import WorkbookRPCViewUnifiedNebiusIL
from bi_external_api.http_views.workbook_rpc_ya_team import WorkbookRPCViewYaTeam
from bi_external_api.settings import ExternalAPISettings
from bi_external_api.utils import init_sentry

LOGGER = logging.getLogger(__name__)


def create_app(settings: ExternalAPISettings) -> web.Application:
    assert settings.DATASET_CONTROL_PLANE_API_BASE_URL is not None
    app_type = settings.APP_TYPE

    secret_sentry_dsn = settings.SENTRY_DSN

    init_sentry(secret_sentry_dsn)

    auth_mw_list: Sequence[Any]

    if app_type == AppType.INTRANET:
        auth_mw_list = [
            blackbox_auth_middleware(),
        ]
    elif app_type in (AppType.DATA_CLOUD, AppType.CLOUD, AppType.NEBIUS):
        yc_auth_settings = settings.YC_AUTH_SETTINGS
        assert yc_auth_settings
        dc_yc_auth_service = YCAuthService(
            allowed_folder_ids=None,
            yc_token_header_mode=YcTokenHeaderMode.EXTERNAL,
            authorization_mode=AuthorizationModeDataCloud(
                project_permission_to_check=yc_auth_settings.YC_AUTHORIZE_PERMISSION,
            ),
            enable_cookie_auth=False,
            access_service_cfg=make_default_yc_auth_service_config(
                endpoint=yc_auth_settings.YC_AS_ENDPOINT,
            ),
            skip_tenant_resolution=True,
        )
        auth_mw_list = [dc_yc_auth_service.middleware]
    elif app_type == AppType.TESTS:
        auth_mw_list = [
            auth_trust_middleware(
                fake_user_id='_the_tests_asyncapp_user_id_',
                fake_user_name='_the_tests_asyncapp_user_name_',
            )
        ]
    else:
        raise ValueError(f"Can not determine auth_mw_list due to unknown app type: {app_type}")

    req_id_service = RequestId(
        append_own_req_id=True,
        app_prefix="ext",
        dl_request_cls=ExtAPIRequest,
    )

    error_handler = ExternalAPIErrorHandler(
        use_sentry=False,
        sentry_app_name_tag="ext-api",
    )

    use_wb_api = (settings.API_TYPE in (
        ExtAPIType.DC,
        ExtAPIType.UNIFIED_DC,
        ExtAPIType.UNIFIED_NEBIUS_IL,
    ))

    middleware_list = [
        TracingService().middleware,
        RequestBootstrap(
            req_id_service=req_id_service,
            error_handler=error_handler,
        ).middleware,
        *auth_mw_list,
        commit_rci_middleware(),
        # TODO FIX: Add when json_body_middleware will be moved to bi_core
        # json_body_middleware(),
        InternalAPIClientsMiddleware(
            dataset_api_base_url=settings.DATASET_CONTROL_PLANE_API_BASE_URL,
            dash_api_base_url=settings.DASH_API_BASE_URL,
            charts_api_base_url=settings.CHARTS_API_BASE_URL,
            us_base_url=settings.US_BASE_URL,
            us_use_workbook_api=use_wb_api,
            force_close_http_conn=settings.INT_API_CLI_FORCE_CLOSE_HTTP_CONN,
            us_master_token=settings.US_MASTER_TOKEN,
        ).middleware
    ]

    app = web.Application(
        middlewares=middleware_list,  # type: ignore
    )
    app.on_response_prepare.append(req_id_service.on_response_prepare)
    AppConfig(
        use_workbooks_api=use_wb_api,
        api_type=settings.API_TYPE,
        do_add_exc_message=settings.DO_ADD_EXC_MESSAGE,
    ).bind(app)

    map_api_type_rpc_view_cls: dict[ExtAPIType, Type[WorkbookRPCView]] = {
        ExtAPIType.CORE: WorkbookRPCViewPrivate,
        ExtAPIType.DC: WorkbookRPCViewDoubleCloud,
        ExtAPIType.UNIFIED_NEBIUS_IL: WorkbookRPCViewUnifiedNebiusIL,
        ExtAPIType.YA_TEAM: WorkbookRPCViewYaTeam,
    }

    app.router.add_route('get', '/ping', PingView)
    app.router.add_route('get', '/external_api/v0/workbook/instance/{workbook_id:.*}', WorkbookRestInstanceView)
    app.router.add_route('post', '/external_api/v0/workbook/rpc', map_api_type_rpc_view_cls[settings.API_TYPE])

    return app


def load_settings() -> ExternalAPISettings:
    fallback_resolver = YEnvFallbackConfigResolver(
        installation_map=InstallationsMap,
        env_map=EnvAliasesMap,
    )
    settings = load_settings_from_env_with_fallback_legacy(
        ExternalAPISettings,
        fallback_cfg_resolver=fallback_resolver,
    )
    return settings


async def create_gunicorn_app() -> web.Application:
    configure_logging(
        app_name='bi_external_api',
        use_jaeger_tracer=use_jaeger_tracer(),
        jaeger_service_name=jaeger_service_name_env_aware('bi-external-api'),
    )
    settings = load_settings()
    try:
        LOGGER.info("Creating application instance...")
        app = create_app(settings=settings)
        LOGGER.info("Application instance was created")
        return app
    except Exception:
        LOGGER.exception("Exception during app creation")
        raise


def main() -> None:
    host = os.environ["APP_HOST"]
    port = int(os.environ["APP_PORT"])
    settings = load_settings()
    app = create_app(settings)
    web.run_app(app, host=host, port=port)


if __name__ == "__main__":
    main()
