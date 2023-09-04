import attr
from aiohttp import web

from bi_api_commons.aio.middlewares.commit_rci import commit_rci_middleware
from bi_api_commons.aio.middlewares.request_bootstrap import RequestBootstrap
from bi_api_commons.aio.middlewares.request_id import RequestId
from bi_api_commons.aiohttp.aiohttp_wrappers import DLRequestBase, DLRequestView
from bi_api_commons.tenant_resolver import TenantResolver, CommonTenantResolver

from bi_configs.settings_loaders.loader_env import load_settings_from_env_with_fallback
from bi_core.aio.ping_view import PingView
from bi_task_processor.worker import HealthChecker

from bi_file_uploader_worker_lib.app import FileUploaderWorkerFactory
from bi_file_uploader_worker_lib.settings import FileUploaderWorkerSettings


class FileUploaderWorkerDLRequest(DLRequestBase):
    pass


class HealthCheckFileUploaderWorkerFactory(FileUploaderWorkerFactory[FileUploaderWorkerSettings]):
    def _get_tenant_resolver(self) -> TenantResolver:
        return CommonTenantResolver()  # does not matter in healthcheck


class HealthCheckView(DLRequestView):
    async def get(self) -> web.StreamResponse:
        settings = load_settings_from_env_with_fallback(FileUploaderWorkerSettings)
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

        app.router.add_route('get', '/ping', PingView)
        app.router.add_route('get', '/health_check', HealthCheckView)

        return app
