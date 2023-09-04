import abc
import logging
from typing import TypeVar, Generic

import attr
from aiohttp import web

from bi_api_commons.aio.middlewares.commit_rci import commit_rci_middleware
from bi_api_commons.aio.middlewares.cors import cors_middleware
from bi_api_commons.aio.middlewares.request_bootstrap import RequestBootstrap
from bi_api_commons.aio.middlewares.request_id import RequestId
from bi_api_commons.aio.typing import AIOHTTPMiddleware

from bi_constants.api_constants import DLHeadersCommon

from bi_core.aio.metrics_view import MetricsView
from bi_core.aio.middlewares.csrf import csrf_middleware
from bi_core.aio.middlewares.master_key import master_key_middleware
from bi_core.aio.middlewares.tracing import TracingService
from bi_core.aio.ping_view import PingView
from bi_core.aio.web_app_services.s3 import S3Service
from bi_core.loader import load_bi_core
from bi_task_processor.arq_wrapper import create_arq_redis_settings

from bi_file_uploader_lib.settings_utils import init_redis_service

from bi_file_uploader_api_lib.aiohttp_services.arq_redis import ArqRedisService
from bi_file_uploader_api_lib.aiohttp_services.crypto import CryptoService
from bi_file_uploader_api_lib.aiohttp_services.error_handler import FileUploaderErrorHandler
from bi_file_uploader_api_lib.dl_request import FileUploaderDLRequest
from bi_file_uploader_api_lib.settings import FileUploaderAPISettings
from bi_file_uploader_api_lib.views import files as files_views, sources as sources_views, misc as misc_views


_TSettings = TypeVar('_TSettings', bound=FileUploaderAPISettings)


@attr.s(kw_only=True)
class FileUploaderApiAppFactory(Generic[_TSettings], abc.ABC):
    _settings: _TSettings = attr.ib()

    @abc.abstractmethod
    def get_auth_middlewares(self) -> list[AIOHTTPMiddleware]:
        raise NotImplementedError()

    def set_up_sentry(self, secret_sentry_dsn: str, release: str) -> None:
        import sentry_sdk
        from sentry_sdk.integrations.aiohttp import AioHttpIntegration
        from sentry_sdk.integrations.atexit import AtexitIntegration
        from sentry_sdk.integrations.excepthook import ExcepthookIntegration
        from sentry_sdk.integrations.stdlib import StdlibIntegration
        from sentry_sdk.integrations.modules import ModulesIntegration
        from sentry_sdk.integrations.argv import ArgvIntegration
        from sentry_sdk.integrations.logging import LoggingIntegration
        from sentry_sdk.integrations.threading import ThreadingIntegration

        from bi_api_commons.logging_sentry import cleanup_common_secret_data

        sentry_sdk.init(
            dsn=secret_sentry_dsn,
            default_integrations=False,
            before_send=cleanup_common_secret_data,
            integrations=[
                # # Default
                AtexitIntegration(),
                ExcepthookIntegration(),
                StdlibIntegration(),
                ModulesIntegration(),
                ArgvIntegration(),
                LoggingIntegration(event_level=logging.WARNING),
                ThreadingIntegration(),
                #  # Custom
                AioHttpIntegration(),
            ],
            release=release,
        )

    def create_app(self, app_version: str) -> web.Application:
        load_bi_core()

        if (secret_sentry_dsn := self._settings.SENTRY_DSN) is not None:
            self.set_up_sentry(secret_sentry_dsn, app_version)

        req_id_service = RequestId(
            append_own_req_id=True,
            app_prefix="f",
            dl_request_cls=FileUploaderDLRequest,
        )

        error_handler = FileUploaderErrorHandler(
            use_sentry=(secret_sentry_dsn is not None),
            sentry_app_name_tag="file-uploader",
        )

        middleware_list = [
            TracingService().middleware,
            cors_middleware(
                allow_origins=self._settings.CORS.ALLOWED_ORIGINS,
                expose_headers=self._settings.CORS.EXPOSE_HEADERS,
                allow_headers=self._settings.CORS.ALLOWED_HEADERS,
                allow_credentials=True,
            ),
            RequestBootstrap(
                req_id_service=req_id_service,
                error_handler=error_handler,
            ).middleware,
            master_key_middleware(
                master_key=self._settings.FILE_UPLOADER_MASTER_TOKEN,
                header=DLHeadersCommon.FILE_UPLOADER_MASTER_TOKEN,
            ),
            *self.get_auth_middlewares(),
            commit_rci_middleware(),
            csrf_middleware(
                csrf_header_name=self._settings.CSRF.HEADER_NAME,
                csrf_time_limit=self._settings.CSRF.TIME_LIMIT,
                csrf_secret=self._settings.CSRF.SECRET,
                csrf_methods=self._settings.CSRF.METHODS,
            ),
            # TODO FIX: Add when json_body_middleware will be moved to bi_core
            # json_body_middleware(),
        ]

        app = web.Application(
            middlewares=middleware_list,  # type: ignore
        )
        app.on_response_prepare.append(req_id_service.on_response_prepare)

        redis_service = init_redis_service(self._settings)
        app.on_startup.append(redis_service.init_hook)
        app.on_shutdown.append(redis_service.tear_down_hook)

        s3_service = S3Service(
            access_key_id=self._settings.S3.ACCESS_KEY_ID,
            secret_access_key=self._settings.S3.SECRET_ACCESS_KEY,
            endpoint_url=self._settings.S3.ENDPOINT_URL,
            tmp_bucket_name=self._settings.S3_TMP_BUCKET_NAME,
            persistent_bucket_name=self._settings.S3_PERSISTENT_BUCKET_NAME,
        )
        app.on_startup.append(s3_service.init_hook)
        app.on_shutdown.append(s3_service.tear_down_hook)

        arq_redis_service = ArqRedisService(arq_settings=create_arq_redis_settings(self._settings.REDIS_ARQ))  # type: ignore
        app.on_startup.append(arq_redis_service.init_hook)
        app.on_shutdown.append(arq_redis_service.tear_down_hook)

        CryptoService(crypto_keys_config=self._settings.CRYPTO_KEYS_CONFIG).bind_to_app(app)

        app.router.add_route('get', '/api/v2/ping', PingView)
        app.router.add_route('get', '/api/v2/metrics', MetricsView)

        app.router.add_route('post', '/api/v2/files', files_views.FilesView)
        app.router.add_route('post', '/api/v2/links', files_views.LinksView)
        app.router.add_route('post', '/api/v2/update_connection_data', files_views.UpdateConnectionDataView)
        app.router.add_route('post', '/api/v2/update_connection_data_internal', files_views.InternalUpdateConnectionDataView)
        app.router.add_route('get', '/api/v2/files/{file_id}/status', files_views.FileStatusView)
        app.router.add_route('get', '/api/v2/files/{file_id}/sources', files_views.FileSourcesView)

        app.router.add_route('get', '/api/v2/files/{file_id}/sources/{source_id}/status', sources_views.SourceStatusView)
        app.router.add_route('post', '/api/v2/files/{file_id}/sources/{source_id}', sources_views.SourceInfoView)
        app.router.add_route('post', '/api/v2/files/{file_id}/sources/{source_id}/preview', sources_views.SourcePreviewView)
        app.router.add_route('post', '/api/v2/files/{file_id}/sources/{source_id}/internal_params', sources_views.SourceInternalParamsView)
        app.router.add_route('post', '/api/v2/files/{file_id}/sources/{source_id}/apply_settings', sources_views.SourceApplySettingsView)

        app.router.add_route('post', '/api/v2/cleanup', misc_views.CleanupTenantView)
        app.router.add_route('post', '/api/v2/rename_tenant_files', misc_views.RenameTenantFilesView)

        app['ALLOW_XLSX'] = self._settings.ALLOW_XLSX

        return app
