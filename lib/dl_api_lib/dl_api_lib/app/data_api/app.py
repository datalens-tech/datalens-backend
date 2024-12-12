from __future__ import annotations

import abc
import functools
import logging.config
from typing import (
    Generic,
    TypeVar,
)

from aiohttp import web
from aiohttp.typedefs import Middleware
import attr

from dl_api_commons.aio.middlewares.commit_rci import commit_rci_middleware
from dl_api_commons.aio.middlewares.request_bootstrap import RequestBootstrap
from dl_api_commons.aio.middlewares.request_id import RequestId
from dl_api_commons.aio.middlewares.tracing import TracingService
from dl_api_commons.aio.server_header import ServerHeader
from dl_api_commons.sentry_config import (
    SentryConfig,
    configure_sentry_for_aiohttp,
)
from dl_api_lib.aio.aiohttp_wrappers import (
    AppWrapper,
    DSAPIRequest,
)
from dl_api_lib.aio.middlewares.error_handling_outer import DatasetAPIErrorHandler
from dl_api_lib.aio.middlewares.json_body_middleware import json_body_middleware
from dl_api_lib.app.data_api.resources.dashsql import DashSQLView
from dl_api_lib.app.data_api.resources.dataset.distinct import (
    DatasetDistinctViewV1,
    DatasetDistinctViewV1_5,
    DatasetDistinctViewV2,
)
from dl_api_lib.app.data_api.resources.dataset.fields import DatasetFieldsView
from dl_api_lib.app.data_api.resources.dataset.pivot import DatasetPivotView
from dl_api_lib.app.data_api.resources.dataset.preview import (
    DatasetPreviewViewV1,
    DatasetPreviewViewV1_5,
    DatasetPreviewViewV2,
)
from dl_api_lib.app.data_api.resources.dataset.range import (
    DatasetRangeViewV1,
    DatasetRangeViewV1_5,
    DatasetRangeViewV2,
)
from dl_api_lib.app.data_api.resources.dataset.result import (
    DatasetResultViewV1,
    DatasetResultViewV1_5,
    DatasetResultViewV2,
)
from dl_api_lib.app.data_api.resources.metrics import DSDataApiMetricsView
from dl_api_lib.app.data_api.resources.ping import (
    PingReadyView,
    PingView,
)
from dl_api_lib.app.data_api.resources.typed_query import DashSQLTypedQueryView
from dl_api_lib.app.data_api.resources.unistat import UnistatView
from dl_api_lib.app_common import SRFactoryBuilder
from dl_api_lib.app_settings import DataApiAppSettings
from dl_compeng_pg.compeng_pg_base.data_processor_service_pg import CompEngPgConfig
from dl_configs.connectors_settings import ConnectorSettingsBase
from dl_configs.enums import RedisMode
from dl_constants.enums import (
    ConnectionType,
    ProcessorType,
    RedisInstanceKind,
)
from dl_core.aio.web_app_services.data_processing.factory import make_compeng_service
from dl_core.aio.web_app_services.redis import (
    RedisSentinelService,
    SingleHostSimpleRedisService,
)


LOGGER = logging.getLogger(__name__)


def _log_exc(coro):  # type: ignore  # 2024-01-30 # TODO: Function is missing a type annotation  [no-untyped-def]
    @functools.wraps(coro)
    async def wrapper(*args, **kwargs):  # type: ignore  # 2024-01-30 # TODO: Function is missing a type annotation  [no-untyped-def]
        try:
            return await coro(*args, **kwargs)
        except Exception:
            LOGGER.exception("Unhandled exception in application signal listener", exc_info=True)
            raise

    return wrapper


async def add_connection_close(request: web.Request, response: web.StreamResponse) -> None:
    """
    Fix a keepalive race condition (HTTP server closing the connection when
    HTTP client is sending a next request) by forcing
    single-request-per-connection (telling the client to close the
    connection after the first request).
    """
    response.headers.add("Connection", "close")


@attr.s(frozen=True)
class EnvSetupResult:
    auth_mw_list: list[Middleware] = attr.ib(kw_only=True)
    sr_middleware_list: list[Middleware] = attr.ib(kw_only=True)
    usm_middleware_list: list[Middleware] = attr.ib(kw_only=True)


TDataApiSettings = TypeVar("TDataApiSettings", bound=DataApiAppSettings)


@attr.s(kw_only=True)
class DataApiAppFactory(SRFactoryBuilder, Generic[TDataApiSettings], abc.ABC):
    _settings: TDataApiSettings = attr.ib()

    @abc.abstractmethod
    def get_app_version(self) -> str:
        raise NotImplementedError()

    @abc.abstractmethod
    def set_up_environment(
        self,
        connectors_settings: dict[ConnectionType, ConnectorSettingsBase],
    ) -> EnvSetupResult:
        raise NotImplementedError()

    @property
    @abc.abstractmethod
    def _is_public(self) -> bool:
        raise NotImplementedError()

    @property
    def _is_async_env(self) -> bool:
        return True

    def set_up_routes(self, app: web.Application) -> None:
        app.router.add_route("get", "/ping", PingView)
        app.router.add_route("get", "/ping_ready", PingReadyView)
        app.router.add_route("get", "/unistat/", UnistatView)
        app.router.add_route("get", "/metrics", DSDataApiMetricsView)

        # TODO: at some point the non-'/data/' routes should probably be removed.
        app.router.add_route("post", "/api/v1/connections/{conn_id}/dashsql", DashSQLView)  # FIXME: Remove
        app.router.add_route("post", "/api/data/v1/connections/{conn_id}/dashsql", DashSQLView)
        app.router.add_route("post", "/api/data/v1/connections/{conn_id}/typed_query", DashSQLTypedQueryView)
        app.router.add_route(
            "post", "/api/v1/datasets/{ds_id}/versions/draft/result", DatasetResultViewV1
        )  # FIXME: Remove
        app.router.add_route("post", "/api/data/v1/datasets/{ds_id}/versions/draft/result", DatasetResultViewV1)
        app.router.add_route("post", "/api/data/v1.5/datasets/{ds_id}/result", DatasetResultViewV1_5)
        app.router.add_route("post", "/api/data/v2/datasets/{ds_id}/result", DatasetResultViewV2)
        app.router.add_route(
            "post", "/api/v1/datasets/{ds_id}/versions/draft/values/distinct", DatasetDistinctViewV1
        )  # FIXME: Remove
        app.router.add_route(
            "post", "/api/data/v1/datasets/{ds_id}/versions/draft/values/distinct", DatasetDistinctViewV1
        )
        app.router.add_route("post", "/api/data/v1.5/datasets/{ds_id}/values/distinct", DatasetDistinctViewV1_5)
        app.router.add_route("post", "/api/data/v2/datasets/{ds_id}/values/distinct", DatasetDistinctViewV2)
        app.router.add_route(
            "post", "/api/v1/datasets/{ds_id}/versions/draft/values/range", DatasetRangeViewV1
        )  # FIXME: Remove
        app.router.add_route("post", "/api/data/v1/datasets/{ds_id}/versions/draft/values/range", DatasetRangeViewV1)
        app.router.add_route("post", "/api/data/v1.5/datasets/{ds_id}/values/range", DatasetRangeViewV1_5)
        app.router.add_route("post", "/api/data/v2/datasets/{ds_id}/values/range", DatasetRangeViewV2)
        app.router.add_route(
            "post", "/api/data/v1/datasets/{ds_id}/versions/draft/pivot", DatasetPivotView
        )  # FIXME: Remove
        app.router.add_route("post", "/api/data/v2/datasets/{ds_id}/pivot", DatasetPivotView)
        app.router.add_route("get", "/api/v1/datasets/{ds_id}/fields", DatasetFieldsView)  # FIXME: Remove
        app.router.add_route("get", "/api/data/v1/datasets/{ds_id}/fields", DatasetFieldsView)
        app.router.add_route("get", "/api/data/v2/datasets/{ds_id}/fields", DatasetFieldsView)

        if not self._is_public:
            app.router.add_route("post", "/api/v1/datasets/data/preview", DatasetPreviewViewV1)  # FIXME: Remove
            app.router.add_route("post", "/api/data/v1/datasets/data/preview", DatasetPreviewViewV1)
            app.router.add_route(
                "post", "/api/v1/datasets/{ds_id}/versions/draft/preview", DatasetPreviewViewV1
            )  # FIXME: Remove
            app.router.add_route("post", "/api/data/v1/datasets/{ds_id}/versions/draft/preview", DatasetPreviewViewV1)
            app.router.add_route("post", "/api/data/v1.5/datasets/data/preview", DatasetPreviewViewV1_5)
            app.router.add_route("post", "/api/data/v1.5/datasets/{ds_id}/preview", DatasetPreviewViewV1_5)
            app.router.add_route("post", "/api/data/v2/datasets/data/preview", DatasetPreviewViewV2)
            app.router.add_route("post", "/api/data/v2/datasets/{ds_id}/preview", DatasetPreviewViewV2)

    def set_up_sentry(self) -> None:
        configure_sentry_for_aiohttp(
            SentryConfig(
                dsn=self._settings.SENTRY_DSN,  # type: ignore  # 2024-01-24 # TODO: Argument "dsn" to "SentryConfig" has incompatible type "str | None"; expected "str"  [arg-type]
                release=self.get_app_version(),
            )
        )

    def create_app(
        self,
        connectors_settings: dict[ConnectionType, ConnectorSettingsBase],
    ) -> web.Application:
        if self._settings.SENTRY_ENABLED:
            self.set_up_sentry()

        env_setup_result = self.set_up_environment(
            connectors_settings=connectors_settings,
        )

        req_id_service = RequestId(
            dl_request_cls=DSAPIRequest,
            append_own_req_id=True,
            app_prefix=self._settings.app_prefix,
            is_public_env=self._is_public,
        )

        error_handler = DatasetAPIErrorHandler(
            public_mode=self._is_public,
            use_sentry=self._settings.SENTRY_ENABLED,
            sentry_app_name_tag=self._settings.app_name,
        )

        middleware_list = [
            TracingService().middleware,
            RequestBootstrap(
                req_id_service=req_id_service,
                error_handler=error_handler,
                timeout_sec=self._settings.COMMON_TIMEOUT_SEC,
            ).middleware,
            *env_setup_result.auth_mw_list,
            commit_rci_middleware(),
            *env_setup_result.sr_middleware_list,
            *env_setup_result.usm_middleware_list,
            json_body_middleware(),
        ]

        app = web.Application(
            middlewares=middleware_list,
        )

        wrapper = AppWrapper(
            allow_query_cache_usage=self._settings.CACHES_ON,
            allow_notifications=not self._is_public,
        )
        wrapper.bind(app)

        ServerHeader("DataLensAPI").add_signal_handlers(app)
        app.on_response_prepare.append(req_id_service.on_response_prepare)

        if self._settings.BI_ASYNC_APP_DISABLE_KEEPALIVE:
            app.on_response_prepare.append(add_connection_close)

        if self._settings.CACHES_ON and self._settings.CACHES_REDIS:
            if self._settings.CACHES_REDIS.MODE == RedisMode.single_host:
                redis_server_single_host = SingleHostSimpleRedisService(
                    instance_kind=RedisInstanceKind.caches,
                    url=self._settings.CACHES_REDIS.as_single_host_url(),
                    password=self._settings.CACHES_REDIS.PASSWORD,
                    ssl=self._settings.CACHES_REDIS.SSL,
                )
                app.on_startup.append(_log_exc(redis_server_single_host.init_hook))
                app.on_cleanup.append(_log_exc(redis_server_single_host.tear_down_hook))
            elif self._settings.CACHES_REDIS.MODE == RedisMode.sentinel:
                redis_server_sentinel = RedisSentinelService(
                    instance_kind=RedisInstanceKind.caches,
                    namespace=self._settings.CACHES_REDIS.CLUSTER_NAME,
                    sentinel_hosts=self._settings.CACHES_REDIS.HOSTS,
                    sentinel_port=self._settings.CACHES_REDIS.PORT,
                    db=self._settings.CACHES_REDIS.DB,
                    password=self._settings.CACHES_REDIS.PASSWORD,
                    ssl=self._settings.CACHES_REDIS.SSL,
                )
                app.on_startup.append(_log_exc(redis_server_sentinel.init_hook))
                app.on_cleanup.append(_log_exc(redis_server_sentinel.tear_down_hook))
            else:
                raise ValueError(f"Unknown redis mode {self._settings.CACHES_REDIS.MODE}")

        if self._settings.MUTATIONS_CACHES_ON and self._settings.MUTATIONS_REDIS:
            if self._settings.MUTATIONS_REDIS.MODE == RedisMode.single_host:
                mutations_redis_server_single_host = SingleHostSimpleRedisService(
                    instance_kind=RedisInstanceKind.mutations,
                    url=self._settings.MUTATIONS_REDIS.as_single_host_url(),
                    password=self._settings.MUTATIONS_REDIS.PASSWORD,
                    ssl=self._settings.MUTATIONS_REDIS.SSL,
                )
                app.on_startup.append(_log_exc(mutations_redis_server_single_host.init_hook))
                app.on_cleanup.append(_log_exc(mutations_redis_server_single_host.tear_down_hook))
            else:
                mutations_redis_server_sentinel: SingleHostSimpleRedisService | RedisSentinelService = (
                    RedisSentinelService(
                        instance_kind=RedisInstanceKind.mutations,
                        namespace=self._settings.MUTATIONS_REDIS.CLUSTER_NAME,
                        sentinel_hosts=self._settings.MUTATIONS_REDIS.HOSTS,
                        sentinel_port=self._settings.MUTATIONS_REDIS.PORT,
                        db=self._settings.MUTATIONS_REDIS.DB,
                        password=self._settings.MUTATIONS_REDIS.PASSWORD,
                        ssl=self._settings.MUTATIONS_REDIS.SSL,
                    )
                )
                app.on_startup.append(_log_exc(mutations_redis_server_sentinel.init_hook))
                app.on_cleanup.append(_log_exc(mutations_redis_server_sentinel.tear_down_hook))

        if self._settings.BI_COMPENG_PG_ON and self._settings.BI_COMPENG_PG_URL is not None:
            compeng_service = make_compeng_service(
                processor_type=ProcessorType.ASYNCPG,
                config=CompEngPgConfig(
                    url=self._settings.BI_COMPENG_PG_URL,
                ),
            )
            app.on_startup.append(_log_exc(compeng_service.init_hook))
            app.on_cleanup.append(_log_exc(compeng_service.tear_down_hook))

        # TODO: don't use it again!
        # special hack for gettings bleeding edge users in dashsql
        app["BLEEDING_EDGE_USERS"] = self._settings.BLEEDING_EDGE_USERS

        self.set_up_routes(app=app)

        return app
