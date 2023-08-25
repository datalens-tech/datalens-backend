from __future__ import annotations

import abc
import functools
import logging.config
from typing import TYPE_CHECKING, Optional

from aiohttp import web
import attr

from bi_constants.api_constants import YcTokenHeaderMode
from bi_constants.enums import ProcessorType, RedisInstanceKind

from bi_configs.enums import AppType, RedisMode

from bi_core.connectors.clickhouse.us_connection import ConnectionClickhouse
from bi_connector_chyt_internal.core.us_connection import BaseConnectionCHYTInternal
from bi_core.aio.middlewares.auth_trust_middleware import auth_trust_middleware
from bi_core.aio.middlewares.services_registry import services_registry_middleware
from bi_core.aio.middlewares.tracing import TracingService
from bi_core.aio.middlewares.us_manager import (
    public_us_manager_middleware, public_usm_workaround_middleware,
    service_us_manager_middleware, us_manager_middleware,
)
from bi_core.aio.web_app_services.data_processing.factory import make_compeng_service
from bi_compeng_pg.compeng_pg_base.data_processor_service_pg import CompEngPgConfig
from bi_core.aio.web_app_services.redis import RedisSentinelService, SingleHostSimpleRedisService
from bi_core.aio.web_app_services.server_header import ServerHeader
from bi_core.utils import make_url

from bi_api_commons.yc_access_control_model import (
    AuthorizationModeDataCloud, AuthorizationModeYandexCloud,
)
from bi_api_commons.aio.middlewares.commit_rci import commit_rci_middleware
from bi_api_commons.aio.middlewares.request_bootstrap import RequestBootstrap
from bi_api_commons.aio.middlewares.request_id import RequestId
from bi_api_commons.aio.typing import AIOHTTPMiddleware

from bi_api_commons_ya_cloud.aio.middlewares.yc_auth import YCAuthService, YCEmbedAuthService
from bi_api_commons_ya_cloud.yc_auth import make_default_yc_auth_service_config
from bi_api_commons_ya_team.aio.middlewares.blackbox_auth import blackbox_auth_middleware

from bi_api_lib.aio.aiohttp_wrappers import AppWrapper, DSAPIRequest
from bi_api_lib.aio.middlewares.error_handling_outer import DatasetAPIErrorHandler
from bi_api_lib.aio.middlewares.json_body_middleware import json_body_middleware
from bi_api_lib.aio.middlewares.public_api_key_middleware import public_api_key_middleware
from bi_api_lib.app_common import SRFactoryBuilder
from bi_api_lib.app_common_settings import ConnOptionsMutatorsFactory
from bi_api_lib.app_settings import AsyncAppSettings, TestAppSettings
from bi_api_lib.app.data_api.resources.dashsql import DashSQLView
from bi_api_lib.app.data_api.resources.dataset.distinct import (
    DatasetDistinctViewV1, DatasetDistinctViewV1_5, DatasetDistinctViewV2,
)
from bi_api_lib.app.data_api.resources.dataset.fields import DatasetFieldsView
from bi_api_lib.app.data_api.resources.dataset.pivot import DatasetPivotView
from bi_api_lib.app.data_api.resources.dataset.preview import (
    DatasetPreviewViewV1, DatasetPreviewViewV1_5, DatasetPreviewViewV2,
)
from bi_api_lib.app.data_api.resources.dataset.range import (
    DatasetRangeViewV1, DatasetRangeViewV1_5, DatasetRangeViewV2,
)
from bi_api_lib.app.data_api.resources.dataset.result import (
    DatasetResultViewV1, DatasetResultViewV1_5, DatasetResultViewV2,
)
from bi_api_lib.app.data_api.resources.metrics import DSDataApiMetricsView
from bi_api_lib.app.data_api.resources.ping import PingView, PingReadyView
from bi_api_lib.app.data_api.resources.unistat import UnistatView
from bi_api_lib.loader import load_bi_api_lib

if TYPE_CHECKING:
    from bi_core.connection_models import ConnectOptions
    from bi_core.us_connection_base import ExecutorBasedMixin


LOGGER = logging.getLogger(__name__)


def _log_exc(coro):  # type: ignore  # TODO: fix
    @functools.wraps(coro)
    async def wrapper(*args, **kwargs):  # type: ignore  # TODO: fix
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
    https://st.yandex-team.ru/BI-1752
    """
    response.headers.add('Connection', 'close')


# TODO CONSIDER: Pass all testing workarounds in constructor args


@attr.s(frozen=True)
class EnvSetupResult:
    auth_mw_list: list[AIOHTTPMiddleware] = attr.ib(kw_only=True)
    sr_middleware_list: list[AIOHTTPMiddleware] = attr.ib(kw_only=True)
    usm_middleware_list: list[AIOHTTPMiddleware] = attr.ib(kw_only=True)


class DataApiAppFactory(SRFactoryBuilder, abc.ABC):
    IS_ASYNC_ENV = True

    def get_app_version(self) -> str:
        return ''

    def set_up_sentry(self, setting: AsyncAppSettings) -> None:
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
            dsn=setting.SENTRY_DSN,
            default_integrations=False,
            before_send=cleanup_common_secret_data,
            integrations=[
                # # Default
                AtexitIntegration(),
                ExcepthookIntegration(),
                # https://st.yandex-team.ru/BI-1392
                # DedupeIntegration(),
                StdlibIntegration(),
                ModulesIntegration(),
                ArgvIntegration(),
                LoggingIntegration(event_level=logging.WARNING),
                ThreadingIntegration(),
                #  # Custom
                AioHttpIntegration(),
            ],
            release=self.get_app_version(),
        )

    def set_up_environment(
            self,
            setting: AsyncAppSettings,
            test_setting: Optional[TestAppSettings] = None,
    ) -> EnvSetupResult:
        # TODO: Move the rest of the env-dependent stuff here

        auth_mw_list: list[AIOHTTPMiddleware]
        sr_middleware_list: list[AIOHTTPMiddleware]
        usm_middleware_list: list[AIOHTTPMiddleware]

        conn_opts_factory = ConnOptionsMutatorsFactory()
        sr_factory = self.get_sr_factory(settings=setting, conn_opts_factory=conn_opts_factory)

        # Auth middlewares
        if setting.APP_TYPE == AppType.CLOUD_PUBLIC:
            auth_mw_list = [
                public_api_key_middleware(api_key=setting.PUBLIC_API_KEY),  # type: ignore  # TODO: fix
                public_usm_workaround_middleware(
                    us_base_url=setting.US_BASE_URL,
                    crypto_keys_config=setting.CRYPTO_KEYS_CONFIG,
                    dataset_id_match_info_code='ds_id',
                    conn_id_match_info_code='conn_id',
                    us_public_token=setting.US_PUBLIC_API_TOKEN,  # type: ignore  # TODO: fix
                    us_master_token=setting.US_MASTER_TOKEN,
                ),
            ]
        elif setting.APP_TYPE == AppType.CLOUD:
            yc_auth_settings = setting.YC_AUTH_SETTINGS
            assert yc_auth_settings
            yc_auth_service = YCAuthService(
                allowed_folder_ids=None,
                yc_token_header_mode=YcTokenHeaderMode.INTERNAL,
                authorization_mode=AuthorizationModeYandexCloud(
                    folder_permission_to_check=yc_auth_settings.YC_AUTHORIZE_PERMISSION,
                    organization_permission_to_check=yc_auth_settings.YC_AUTHORIZE_PERMISSION,
                ),
                enable_cookie_auth=False,
                access_service_cfg=make_default_yc_auth_service_config(
                    endpoint=yc_auth_settings.YC_AS_ENDPOINT,
                ),
            )
            auth_mw_list = [yc_auth_service.middleware]
        elif setting.APP_TYPE == AppType.INTRANET:
            auth_mw_list = [
                blackbox_auth_middleware(),
            ]
        elif setting.APP_TYPE == AppType.TESTS:
            if test_setting is not None and test_setting.use_bb_in_test:  # NOTE used only in solomon tests
                auth_mw_list = [
                    blackbox_auth_middleware(tvm_info=test_setting.tvm_info),
                ]
            else:
                auth_mw_list = [
                    auth_trust_middleware(
                        fake_user_id='_the_tests_asyncapp_user_id_',
                        fake_user_name='_the_tests_asyncapp_user_name_',
                    )
                ]
        elif setting.APP_TYPE == AppType.DATA_CLOUD:
            yc_auth_settings = setting.YC_AUTH_SETTINGS
            assert yc_auth_settings
            dc_yc_auth_service = YCAuthService(
                allowed_folder_ids=None,
                yc_token_header_mode=YcTokenHeaderMode.INTERNAL,
                authorization_mode=AuthorizationModeDataCloud(
                    project_permission_to_check=yc_auth_settings.YC_AUTHORIZE_PERMISSION,
                ),
                enable_cookie_auth=False,
                access_service_cfg=make_default_yc_auth_service_config(
                    endpoint=yc_auth_settings.YC_AS_ENDPOINT,
                ),
            )
            auth_mw_list = [dc_yc_auth_service.middleware]
        elif setting.APP_TYPE == AppType.NEBIUS:
            yc_auth_settings = setting.YC_AUTH_SETTINGS
            assert yc_auth_settings
            yc_auth_service = YCAuthService(
                allowed_folder_ids=None,
                yc_token_header_mode=YcTokenHeaderMode.INTERNAL,
                authorization_mode=AuthorizationModeYandexCloud(
                    folder_permission_to_check=yc_auth_settings.YC_AUTHORIZE_PERMISSION,
                    organization_permission_to_check=yc_auth_settings.YC_AUTHORIZE_PERMISSION,
                ),
                enable_cookie_auth=False,
                access_service_cfg=make_default_yc_auth_service_config(
                    endpoint=yc_auth_settings.YC_AS_ENDPOINT,
                ),
            )
            auth_mw_list = [yc_auth_service.middleware]
        elif setting.APP_TYPE == AppType.DATA_CLOUD_EMBED:
            yc_auth_settings = setting.YC_AUTH_SETTINGS
            assert yc_auth_settings
            dc_yc_embed_auth_service = YCEmbedAuthService(
                authorization_mode=AuthorizationModeDataCloud(
                    project_permission_to_check=yc_auth_settings.YC_AUTHORIZE_PERMISSION,
                ),
            )
            auth_mw_list = [dc_yc_embed_auth_service.middleware]
        elif setting.APP_TYPE == AppType.CLOUD_EMBED:
            yc_auth_settings = setting.YC_AUTH_SETTINGS
            assert yc_auth_settings
            yc_embed_auth_service = YCEmbedAuthService(
                authorization_mode=AuthorizationModeYandexCloud(
                    folder_permission_to_check=yc_auth_settings.YC_AUTHORIZE_PERMISSION,
                    organization_permission_to_check=yc_auth_settings.YC_AUTHORIZE_PERMISSION,
                ),
            )
            auth_mw_list = [yc_embed_auth_service.middleware]
        else:
            raise ValueError(f"Unsupported auth mode: {setting.APP_TYPE}")

        # SR middlewares

        def ignore_managed_conn_opts_mutator(
                conn_opts: ConnectOptions, conn: ExecutorBasedMixin
        ) -> Optional[ConnectOptions]:
            if setting.MDB_FORCE_IGNORE_MANAGED_NETWORK:
                return conn_opts.clone(use_managed_network=False)
            return None

        if setting.APP_TYPE in (AppType.CLOUD, AppType.CLOUD_EMBED, AppType.NEBIUS):

            conn_opts_factory.add_mutator(ignore_managed_conn_opts_mutator)

            sr_middleware_list = [
                services_registry_middleware(
                    services_registry_factory=sr_factory,
                    use_query_cache=setting.CACHES_ON,
                    use_mutation_cache=setting.MUTATIONS_CACHES_ON,
                    mutation_cache_default_ttl=setting.MUTATIONS_CACHES_DEFAULT_TTL,
                ),
            ]
        elif setting.APP_TYPE == AppType.CLOUD_PUBLIC:

            def public_timeout_conn_opts_mutator(
                    conn_opts: ConnectOptions, conn: ExecutorBasedMixin
            ) -> Optional[ConnectOptions]:
                if setting.PUBLIC_CH_QUERY_TIMEOUT is not None:
                    if isinstance(conn, ConnectionClickhouse):
                        return conn_opts.clone(
                            total_timeout=setting.PUBLIC_CH_QUERY_TIMEOUT,
                            max_execution_time=setting.PUBLIC_CH_QUERY_TIMEOUT - 2,
                        )
                return None

            conn_opts_factory.add_mutator(public_timeout_conn_opts_mutator)
            conn_opts_factory.add_mutator(ignore_managed_conn_opts_mutator)

            sr_middleware_list = [
                services_registry_middleware(
                    services_registry_factory=sr_factory,
                    use_query_cache=setting.CACHES_ON,
                    use_mutation_cache=setting.MUTATIONS_CACHES_ON,
                    mutation_cache_default_ttl=setting.MUTATIONS_CACHES_DEFAULT_TTL,
                ),
            ]
        elif setting.APP_TYPE == AppType.INTRANET:
            def chyt_mirroring_conn_opts_mutator(
                    conn_opts: ConnectOptions, conn: ExecutorBasedMixin
            ) -> Optional[ConnectOptions]:
                if not isinstance(conn, BaseConnectionCHYTInternal):
                    return None
                mirroring_config = setting.CHYT_MIRRORING

                if mirroring_config is None:
                    return None
                conn_dto = conn.get_conn_dto()
                cluster_name = conn_dto.cluster.lower()  # type: ignore  # TODO: fix
                mirroring_clique_alias = (
                    mirroring_config.MAP.get(
                        (cluster_name, conn_dto.clique_alias)  # type: ignore  # TODO: fix
                    ) or mirroring_config.MAP.get((cluster_name, None))
                )
                if not mirroring_clique_alias:
                    return None
                return conn_opts.clone(
                    mirroring_frac=mirroring_config.FRAC,
                    mirroring_clique_req_timeout_sec=mirroring_config.REQ_TIMEOUT_SEC,
                    mirroring_clique_alias=mirroring_clique_alias,
                )

            conn_opts_factory.add_mutator(chyt_mirroring_conn_opts_mutator)

            sr_middleware_list = [
                services_registry_middleware(
                    services_registry_factory=sr_factory,
                    use_query_cache=setting.CACHES_ON,
                    use_mutation_cache=setting.MUTATIONS_CACHES_ON,
                    mutation_cache_default_ttl=setting.MUTATIONS_CACHES_DEFAULT_TTL,
                ),
            ]
        elif setting.APP_TYPE in (AppType.DATA_CLOUD, AppType.DATA_CLOUD_EMBED):
            sr_middleware_list = [
                services_registry_middleware(
                    services_registry_factory=sr_factory,
                    use_query_cache=setting.CACHES_ON,
                    use_mutation_cache=setting.MUTATIONS_CACHES_ON,
                    mutation_cache_default_ttl=setting.MUTATIONS_CACHES_DEFAULT_TTL,
                ),
            ]
        elif setting.APP_TYPE == AppType.TESTS:
            sr_middleware_list = [
                services_registry_middleware(
                    services_registry_factory=sr_factory,
                    use_query_cache=setting.CACHES_ON,
                    use_mutation_cache=setting.MUTATIONS_CACHES_ON,
                    mutation_cache_default_ttl=setting.MUTATIONS_CACHES_DEFAULT_TTL,
                ),
            ]
        else:
            raise ValueError(f"Unsupported auth mode: {setting.APP_TYPE}")

        # US manager middleware list
        common_us_kw = dict(
            us_base_url=setting.US_BASE_URL,
            crypto_keys_config=setting.CRYPTO_KEYS_CONFIG,
        )
        if setting.APP_TYPE == AppType.CLOUD_PUBLIC:
            usm_middleware_list = [
                public_us_manager_middleware(
                    us_public_token=setting.US_PUBLIC_API_TOKEN, **common_us_kw  # type: ignore  # TODO: fix
                ),
            ]

        elif setting.APP_TYPE == AppType.TESTS:
            usm_middleware_list = [
                service_us_manager_middleware(us_master_token=setting.US_MASTER_TOKEN, **common_us_kw),  # type: ignore  # TODO: fix
                service_us_manager_middleware(us_master_token=setting.US_MASTER_TOKEN, as_user_usm=True, **common_us_kw),  # type: ignore  # TODO: fix
            ]

        elif setting.APP_TYPE in (AppType.DATA_CLOUD_EMBED, AppType.CLOUD_EMBED):
            usm_middleware_list = [
                us_manager_middleware(embed=True, **common_us_kw),  # type: ignore  # TODO: fix
                service_us_manager_middleware(us_master_token=setting.US_MASTER_TOKEN, **common_us_kw)  # type: ignore  # TODO: fix
            ]

        else:
            usm_middleware_list = [
                us_manager_middleware(**common_us_kw),  # type: ignore  # TODO: fix
                service_us_manager_middleware(us_master_token=setting.US_MASTER_TOKEN, **common_us_kw)  # type: ignore  # TODO: fix
            ]

        result = EnvSetupResult(
            auth_mw_list=auth_mw_list,
            sr_middleware_list=sr_middleware_list,
            usm_middleware_list=usm_middleware_list,
        )

        return result

    def set_up_routes(
            self,
            app: web.Application,
            setting: AsyncAppSettings,
    ) -> None:

        # Routes
        app.router.add_route('get', '/ping', PingView)
        app.router.add_route('get', '/ping_ready', PingReadyView)
        app.router.add_route('get', '/unistat/', UnistatView)
        app.router.add_route('get', '/metrics', DSDataApiMetricsView)

        # TODO: at some point the non-'/data/' routes should probably be removed.
        app.router.add_route('post', '/api/v1/connections/{conn_id}/dashsql', DashSQLView)  # FIXME: Remove
        app.router.add_route('post', '/api/data/v1/connections/{conn_id}/dashsql', DashSQLView)
        app.router.add_route('post', '/api/v1/datasets/{ds_id}/versions/draft/result', DatasetResultViewV1)  # FIXME: Remove
        app.router.add_route('post', '/api/data/v1/datasets/{ds_id}/versions/draft/result', DatasetResultViewV1)
        app.router.add_route('post', '/api/data/v1.5/datasets/{ds_id}/result', DatasetResultViewV1_5)
        app.router.add_route('post', '/api/data/v2/datasets/{ds_id}/result', DatasetResultViewV2)
        app.router.add_route('post', '/api/v1/datasets/{ds_id}/versions/draft/values/distinct', DatasetDistinctViewV1)  # FIXME: Remove
        app.router.add_route('post', '/api/data/v1/datasets/{ds_id}/versions/draft/values/distinct', DatasetDistinctViewV1)
        app.router.add_route('post', '/api/data/v1.5/datasets/{ds_id}/values/distinct', DatasetDistinctViewV1_5)
        app.router.add_route('post', '/api/data/v2/datasets/{ds_id}/values/distinct', DatasetDistinctViewV2)
        app.router.add_route('post', '/api/v1/datasets/{ds_id}/versions/draft/values/range', DatasetRangeViewV1)  # FIXME: Remove
        app.router.add_route('post', '/api/data/v1/datasets/{ds_id}/versions/draft/values/range', DatasetRangeViewV1)
        app.router.add_route('post', '/api/data/v1.5/datasets/{ds_id}/values/range', DatasetRangeViewV1_5)
        app.router.add_route('post', '/api/data/v2/datasets/{ds_id}/values/range', DatasetRangeViewV2)
        app.router.add_route('post', '/api/data/v1/datasets/{ds_id}/versions/draft/pivot', DatasetPivotView)  # FIXME: Remove
        app.router.add_route('post', '/api/data/v2/datasets/{ds_id}/pivot', DatasetPivotView)
        app.router.add_route('get', '/api/v1/datasets/{ds_id}/fields', DatasetFieldsView)  # FIXME: Remove
        app.router.add_route('get', '/api/data/v1/datasets/{ds_id}/fields', DatasetFieldsView)
        app.router.add_route('get', '/api/data/v2/datasets/{ds_id}/fields', DatasetFieldsView)

        if setting.APP_TYPE != AppType.CLOUD_PUBLIC:
            app.router.add_route('post', '/api/v1/datasets/data/preview', DatasetPreviewViewV1)  # FIXME: Remove
            app.router.add_route('post', '/api/data/v1/datasets/data/preview', DatasetPreviewViewV1)
            app.router.add_route('post', '/api/v1/datasets/{ds_id}/versions/draft/preview', DatasetPreviewViewV1)  # FIXME: Remove
            app.router.add_route('post', '/api/data/v1/datasets/{ds_id}/versions/draft/preview', DatasetPreviewViewV1)
            app.router.add_route('post', '/api/data/v1.5/datasets/data/preview', DatasetPreviewViewV1_5)
            app.router.add_route('post', '/api/data/v1.5/datasets/{ds_id}/preview', DatasetPreviewViewV1_5)
            app.router.add_route('post', '/api/data/v2/datasets/data/preview', DatasetPreviewViewV2)
            app.router.add_route('post', '/api/data/v2/datasets/{ds_id}/preview', DatasetPreviewViewV2)

    def create_app(
            self,
            setting: AsyncAppSettings,
            test_setting: Optional[TestAppSettings] = None
    ) -> web.Application:
        if setting.SENTRY_ENABLED:
            self.set_up_sentry(setting=setting)

        load_bi_api_lib()

        env_setup_result = self.set_up_environment(
            setting=setting, test_setting=test_setting,
        )

        req_id_service = RequestId(
            dl_request_cls=DSAPIRequest,
            append_own_req_id=True,
            app_prefix=setting.app_prefix,
            is_public_env=(setting.APP_TYPE == AppType.CLOUD_PUBLIC),  # FIXME: remove APP_TYPE
        )

        error_handler = DatasetAPIErrorHandler(
            public_mode=(setting.APP_TYPE in (AppType.CLOUD_PUBLIC, AppType.CLOUD_EMBED, AppType.DATA_CLOUD_EMBED)),  # FIXME: remove APP_TYPE
            use_sentry=setting.SENTRY_ENABLED,
            sentry_app_name_tag=setting.app_name,
        )

        middleware_list = [
            TracingService().middleware,
            RequestBootstrap(
                req_id_service=req_id_service,
                error_handler=error_handler,
                timeout_sec=setting.COMMON_TIMEOUT_SEC,
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
            allow_query_cache_usage=setting.CACHES_ON,
            allow_notifications=(setting.APP_TYPE != AppType.CLOUD_PUBLIC),  # FIXME: remove APP_TYPE
        )
        wrapper.bind(app)

        ServerHeader("DataLensAPI").add_signal_handlers(app)
        app.on_response_prepare.append(req_id_service.on_response_prepare)

        if setting.BI_ASYNC_APP_DISABLE_KEEPALIVE:
            app.on_response_prepare.append(add_connection_close)

        # Redis
        if setting.CACHES_ON and setting.CACHES_REDIS:
            if setting.CACHES_REDIS.MODE == RedisMode.single_host:
                redis_server_single_host = SingleHostSimpleRedisService(
                    instance_kind=RedisInstanceKind.caches,
                    url=make_url(
                        protocol="rediss" if setting.CACHES_REDIS.SSL else "redis",
                        host=setting.CACHES_REDIS.HOSTS[0],
                        port=setting.CACHES_REDIS.PORT,
                        path=str(setting.CACHES_REDIS.DB),
                    ),
                    password=setting.CACHES_REDIS.PASSWORD,
                    ssl=setting.CACHES_REDIS.SSL,
                )
                app.on_startup.append(_log_exc(redis_server_single_host.init_hook))
                app.on_cleanup.append(_log_exc(redis_server_single_host.tear_down_hook))
            elif setting.CACHES_REDIS.MODE == RedisMode.sentinel:
                redis_server_sentinel = RedisSentinelService(
                    instance_kind=RedisInstanceKind.caches,
                    namespace=setting.CACHES_REDIS.CLUSTER_NAME,
                    sentinel_hosts=setting.CACHES_REDIS.HOSTS,
                    sentinel_port=setting.CACHES_REDIS.PORT,
                    db=setting.CACHES_REDIS.DB,
                    password=setting.CACHES_REDIS.PASSWORD,
                    ssl=setting.CACHES_REDIS.SSL,
                )
                app.on_startup.append(_log_exc(redis_server_sentinel.init_hook))
                app.on_cleanup.append(_log_exc(redis_server_sentinel.tear_down_hook))
            else:
                raise ValueError(f'Unknown redis mode {setting.CACHES_REDIS.MODE}')

        if setting.MUTATIONS_CACHES_ON and setting.MUTATIONS_REDIS:
            if setting.MUTATIONS_REDIS.MODE == RedisMode.single_host:
                mutations_redis_server_single_host = SingleHostSimpleRedisService(
                    instance_kind=RedisInstanceKind.mutations,
                    url=make_url(
                        protocol="rediss" if setting.MUTATIONS_REDIS.SSL else "redis",
                        host=setting.MUTATIONS_REDIS.HOSTS[0],
                        port=setting.MUTATIONS_REDIS.PORT,
                        path=str(setting.MUTATIONS_REDIS.DB),
                    ),
                    password=setting.MUTATIONS_REDIS.PASSWORD,
                    ssl=setting.MUTATIONS_REDIS.SSL,
                )
                app.on_startup.append(_log_exc(mutations_redis_server_single_host.init_hook))
                app.on_cleanup.append(_log_exc(mutations_redis_server_single_host.tear_down_hook))
            else:
                mutations_redis_server_sentinel: SingleHostSimpleRedisService | RedisSentinelService = RedisSentinelService(
                    instance_kind=RedisInstanceKind.mutations,
                    namespace=setting.MUTATIONS_REDIS.CLUSTER_NAME,
                    sentinel_hosts=setting.MUTATIONS_REDIS.HOSTS,
                    sentinel_port=setting.MUTATIONS_REDIS.PORT,
                    db=setting.MUTATIONS_REDIS.DB,
                    password=setting.MUTATIONS_REDIS.PASSWORD,
                    ssl=setting.MUTATIONS_REDIS.SSL,
                )
                app.on_startup.append(_log_exc(mutations_redis_server_sentinel.init_hook))
                app.on_cleanup.append(_log_exc(mutations_redis_server_sentinel.tear_down_hook))

        # Compeng
        if setting.BI_COMPENG_PG_ON and setting.BI_COMPENG_PG_URL is not None:
            compeng_service = make_compeng_service(
                processor_type=ProcessorType.ASYNCPG,
                config=CompEngPgConfig(
                    url=setting.BI_COMPENG_PG_URL,
                ),
            )
            app.on_startup.append(_log_exc(compeng_service.init_hook))
            app.on_cleanup.append(_log_exc(compeng_service.tear_down_hook))

        # TODO: don't use it again!
        # special hack for gettings bleeding edge users in dashsql
        # https://st.yandex-team.ru/BI-3114
        app['settings'] = setting

        # Routes
        self.set_up_routes(app=app, setting=setting)

        return app
