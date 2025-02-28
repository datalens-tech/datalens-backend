import abc
from typing import (
    Generic,
    Type,
    TypeVar,
)

from aiohttp import web
from aiohttp.typedefs import Middleware
import attr

from dl_api_commons.aio.middlewares.commit_rci import commit_rci_middleware
from dl_api_commons.aio.middlewares.cors import cors_middleware
from dl_api_commons.aio.middlewares.csrf import CSRFMiddleware
from dl_api_commons.aio.middlewares.master_key import master_key_middleware
from dl_api_commons.aio.middlewares.request_bootstrap import RequestBootstrap
from dl_api_commons.aio.middlewares.request_id import RequestId
from dl_api_commons.aio.middlewares.tracing import TracingService
from dl_api_commons.sentry_config import (
    SentryConfig,
    configure_sentry_for_aiohttp,
)
from dl_constants.api_constants import DLHeadersCommon
from dl_core.aio.metrics_view import MetricsView
from dl_core.aio.ping_view import PingView
from dl_core.loader import (
    CoreLibraryConfig,
    load_core_lib,
)
from dl_file_uploader_api_lib.aiohttp_services.arq_redis import ArqRedisService
from dl_file_uploader_api_lib.aiohttp_services.crypto import CryptoService
from dl_file_uploader_api_lib.aiohttp_services.error_handler import FileUploaderErrorHandler
from dl_file_uploader_api_lib.dl_request import FileUploaderDLRequest
from dl_file_uploader_api_lib.settings import FileUploaderAPISettings
from dl_file_uploader_api_lib.views import files as files_views
from dl_file_uploader_api_lib.views import misc as misc_views
from dl_file_uploader_api_lib.views import sources as sources_views
from dl_file_uploader_lib.settings_utils import init_redis_service
from dl_s3.s3_service import S3Service
from dl_task_processor.arq_wrapper import create_arq_redis_settings


_TSettings = TypeVar("_TSettings", bound=FileUploaderAPISettings)


@attr.s(kw_only=True)
class FileUploaderApiAppFactory(Generic[_TSettings], abc.ABC):
    CSRF_MIDDLEWARE_CLS: Type[CSRFMiddleware] = CSRFMiddleware

    _settings: _TSettings = attr.ib()

    @abc.abstractmethod
    def get_auth_middlewares(self) -> list[Middleware]:
        raise NotImplementedError()

    def set_up_sentry(self, secret_sentry_dsn: str, release: str) -> None:
        configure_sentry_for_aiohttp(
            SentryConfig(
                dsn=secret_sentry_dsn,
                release=release,
            )
        )

    def create_app(self, app_version: str) -> web.Application:
        core_conn_whitelist = ["clickhouse", "file", "gsheets_v2"]
        load_core_lib(core_lib_config=CoreLibraryConfig(core_connector_ep_names=core_conn_whitelist))

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

        middleware_list: list[Middleware] = [
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
            self.CSRF_MIDDLEWARE_CLS(
                csrf_header_name=self._settings.CSRF.HEADER_NAME,
                csrf_time_limit=self._settings.CSRF.TIME_LIMIT,
                csrf_secrets=self._settings.CSRF.SECRET,
                csrf_methods=self._settings.CSRF.METHODS,
            ).middleware,
            # TODO FIX: Add when json_body_middleware will be moved to dl_core
            # json_body_middleware(),
        ]

        app = web.Application(
            middlewares=middleware_list,
        )
        app.on_response_prepare.append(req_id_service.on_response_prepare)

        redis_service = init_redis_service(self._settings)
        app.on_startup.append(redis_service.init_hook)
        app.on_shutdown.append(redis_service.tear_down_hook)

        s3_service = S3Service(
            access_key_id=self._settings.S3.ACCESS_KEY_ID,
            secret_access_key=self._settings.S3.SECRET_ACCESS_KEY,
            endpoint_url=self._settings.S3.ENDPOINT_URL,
            use_virtual_host_addressing=self._settings.S3.USE_VIRTUAL_HOST_ADDRESSING,
            tmp_bucket_name=self._settings.S3_TMP_BUCKET_NAME,
            persistent_bucket_name=self._settings.S3_PERSISTENT_BUCKET_NAME,
        )
        app.on_startup.append(s3_service.init_hook)
        app.on_shutdown.append(s3_service.tear_down_hook)

        arq_redis_service = ArqRedisService(arq_settings=create_arq_redis_settings(self._settings.REDIS_ARQ))
        app.on_startup.append(arq_redis_service.init_hook)
        app.on_shutdown.append(arq_redis_service.tear_down_hook)

        CryptoService(crypto_keys_config=self._settings.CRYPTO_KEYS_CONFIG).bind_to_app(app)

        app.router.add_route("get", "/api/v2/ping", PingView)
        app.router.add_route("get", "/api/v2/metrics", MetricsView)

        app.router.add_route("post", "/api/v2/files", files_views.FilesView)
        app.router.add_route("get", "/api/v2/make_presigned_url", files_views.MakePresignedUrlView)
        app.router.add_route("post", "/api/v2/download_presigned_url", files_views.DownloadPresignedUrlView)
        app.router.add_route("post", "/api/v2/links", files_views.LinksView)
        app.router.add_route("post", "/api/v2/documents", files_views.DocumentsView)
        app.router.add_route("post", "/api/v2/update_connection_data", files_views.UpdateConnectionDataView)
        app.router.add_route(
            "post", "/api/v2/update_connection_data_internal", files_views.InternalUpdateConnectionDataView
        )
        app.router.add_route("get", "/api/v2/files/{file_id}/status", files_views.FileStatusView)
        app.router.add_route("get", "/api/v2/files/{file_id}/sources", files_views.FileSourcesView)

        app.router.add_route(
            "get", "/api/v2/files/{file_id}/sources/{source_id}/status", sources_views.SourceStatusView
        )
        app.router.add_route("post", "/api/v2/files/{file_id}/sources/{source_id}", sources_views.SourceInfoView)
        app.router.add_route(
            "post", "/api/v2/files/{file_id}/sources/{source_id}/preview", sources_views.SourcePreviewView
        )
        app.router.add_route(
            "post",
            "/api/v2/files/{file_id}/sources/{source_id}/internal_params",
            sources_views.SourceInternalParamsView,
        )
        app.router.add_route(
            "post", "/api/v2/files/{file_id}/sources/{source_id}/apply_settings", sources_views.SourceApplySettingsView
        )

        app.router.add_route("post", "/api/v2/cleanup", misc_views.CleanupTenantView)
        app.router.add_route("post", "/api/v2/rename_tenant_files", misc_views.RenameTenantFilesView)

        app["ALLOW_XLSX"] = self._settings.ALLOW_XLSX

        return app
