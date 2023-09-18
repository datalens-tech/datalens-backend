from aiohttp import web
import attr

from bi_defaults.environments import (
    EnvAliasesMap,
    InstallationsMap,
)
from dl_api_commons.aio.middlewares.commit_rci import commit_rci_middleware
from dl_api_commons.aio.middlewares.request_bootstrap import RequestBootstrap
from dl_api_commons.aio.middlewares.request_id import RequestId
from dl_api_commons.aiohttp.aiohttp_wrappers import (
    DLRequestBase,
    DLRequestView,
)
from dl_api_commons.tenant_resolver import (
    CommonTenantResolver,
    TenantResolver,
)
from dl_configs.settings_loaders.fallback_cfg_resolver import YEnvFallbackConfigResolver
from dl_configs.settings_loaders.loader_env import load_settings_from_env_with_fallback_legacy
from dl_core.aio.ping_view import PingView
from dl_file_uploader_worker_lib.app import FileUploaderWorkerFactory
from dl_file_uploader_worker_lib.settings import FileUploaderWorkerSettings
from dl_task_processor.worker import HealthChecker


class FileUploaderWorkerDLRequest(DLRequestBase):
    pass


class HealthCheckFileUploaderWorkerFactory(FileUploaderWorkerFactory[FileUploaderWorkerSettings]):
    def _get_tenant_resolver(self) -> TenantResolver:
        return CommonTenantResolver()  # does not matter in healthcheck


class HealthCheckView(DLRequestView):
    async def get(self) -> web.StreamResponse:
        fallback_resolver = YEnvFallbackConfigResolver(
            installation_map=InstallationsMap,
            env_map=EnvAliasesMap,
        )
        settings = load_settings_from_env_with_fallback_legacy(
            FileUploaderWorkerSettings,
            fallback_cfg_resolver=fallback_resolver,
        )
        worker = HealthCheckFileUploaderWorkerFactory(settings=settings).create_worker()
        health_checker = HealthChecker(worker)
        await health_checker.check_and_raise()
        return web.Response()


@attr.s(kw_only=True)
class FileUploaderWorkerHealthCheckAppFactory:
    def create_app(self) -> web.Application:
        req_id_service = RequestId(
            append_own_req_id=True,
            app_prefix="fuwhc",
            dl_request_cls=FileUploaderWorkerDLRequest,
        )
        middleware_list = [
            RequestBootstrap(req_id_service=req_id_service).middleware,
            commit_rci_middleware(),
        ]

        app = web.Application(
            middlewares=middleware_list,  # type: ignore
        )

        app.on_response_prepare.append(req_id_service.on_response_prepare)

        app.router.add_route("get", "/ping", PingView)
        app.router.add_route("get", "/health_check", HealthCheckView)

        return app
