from __future__ import annotations

from typing import Optional

from bi_configs.enums import RequiredService, RQE_SERVICES

from bi_core.aio.middlewares.services_registry import services_registry_middleware
from bi_core.aio.middlewares.us_manager import us_manager_middleware, service_us_manager_middleware
from bi_core.connection_models import ConnectOptions
from bi_core.data_processing.cache.primitives import CacheTTLConfig
from bi_core.services_registry.entity_checker import EntityUsageChecker
from bi_core.services_registry.env_manager_factory import CloudEnvManagerFactory
from bi_core.services_registry.env_manager_factory_base import EnvManagerFactory
from bi_core.services_registry.inst_specific_sr import InstallationSpecificServiceRegistryFactory
from bi_core.services_registry.rqe_caches import RQECachesSetting
from bi_core.services_registry.sa_creds import SACredsSettings, SACredsRetrieverFactory
from bi_core.us_connection_base import ExecutorBasedMixin

from bi_api_lib.app.data_api.app import DataApiAppFactory, EnvSetupResult
from bi_api_lib.app_common import SRFactoryBuilder
from bi_api_lib.app_common_settings import ConnOptionsMutatorsFactory
from bi_api_lib.app_settings import BaseAppSettings, AsyncAppSettings, TestAppSettings
from bi_api_lib.connector_availability.base import ConnectorAvailabilityConfig

from bi_api_commons.aio.typing import AIOHTTPMiddleware
from bi_api_commons.yc_access_control_model import AuthorizationModeYandexCloud
from bi_api_commons_ya_cloud.aio.middlewares.yc_auth import YCEmbedAuthService
from bi_service_registry_ya_cloud.yc_service_registry import YCServiceRegistryFactory

from app_yc_data_api_sec_embeds import app_version


class DataApiSecEmbedsSRFactoryBuilderYC(SRFactoryBuilder):
    def _get_required_services(self, settings: BaseAppSettings) -> set[RequiredService]:
        return set(RQE_SERVICES)

    def _get_env_manager_factory(self, settings: BaseAppSettings) -> EnvManagerFactory:
        return CloudEnvManagerFactory(samples_ch_hosts=list(settings.SAMPLES_CH_HOSTS))

    def _get_inst_specific_sr_factory(
            self,
            settings: BaseAppSettings,
    ) -> Optional[InstallationSpecificServiceRegistryFactory]:
        sa_creds_settings = SACredsSettings(
            mode=settings.YC_SA_CREDS_MODE,
            env_key_data=settings.YC_SA_CREDS_KEY_DATA,
        ) if settings.YC_SA_CREDS_MODE is not None else None

        return YCServiceRegistryFactory(
            yc_billing_host=settings.YC_BILLING_HOST,
            yc_api_endpoint_rm=settings.YC_RM_CP_ENDPOINT,
            yc_as_endpoint=settings.YC_AUTH_SETTINGS.YC_AS_ENDPOINT if settings.YC_AUTH_SETTINGS else None,
            yc_api_endpoint_iam=settings.YC_AUTH_SETTINGS.YC_API_ENDPOINT_IAM if settings.YC_AUTH_SETTINGS else None,
            yc_ts_endpoint=settings.YC_IAM_TS_ENDPOINT,
            sa_creds_retriever_factory=SACredsRetrieverFactory(
                sa_creds_settings=sa_creds_settings,
                ts_endpoint=settings.YC_IAM_TS_ENDPOINT
            ) if sa_creds_settings else None,
            blackbox_name=settings.BLACKBOX_NAME,
        )

    def _get_entity_usage_checker(self, settings: BaseAppSettings) -> Optional[EntityUsageChecker]:
        return None

    def _get_bleeding_edge_users(self, settings: BaseAppSettings) -> tuple[str, ...]:
        return tuple()

    def _get_rqe_caches_settings(self, settings: AsyncAppSettings) -> Optional[RQECachesSetting]:  # type: ignore[override]
        if settings.RQE_CACHES_ON:
            return RQECachesSetting(
                redis_settings=settings.RQE_CACHES_REDIS,
                caches_ttl=settings.RQE_CACHES_TTL,
            )
        return None

    def _get_default_cache_ttl_settings(self, settings: AsyncAppSettings) -> Optional[CacheTTLConfig]:  # type: ignore[override]
        if settings.CACHES_TTL_SETTINGS is not None:
            # FIXME: resolve typing mismatch for CacheTTLConfig
            return CacheTTLConfig(
                ttl_sec_direct=settings.CACHES_TTL_SETTINGS.OTHER,  # type: ignore  # TODO: fix
                ttl_sec_materialized=settings.CACHES_TTL_SETTINGS.MATERIALIZED,  # type: ignore  # TODO: fix
            )
        return None

    def _get_connector_availability(self, settings: BaseAppSettings) -> Optional[ConnectorAvailabilityConfig]:
        return None


class DataApiSecEmbedsAppFactoryYC(DataApiAppFactory, DataApiSecEmbedsSRFactoryBuilderYC):
    def get_app_version(self) -> str:
        return app_version

    def set_up_environment(
            self,
            setting: AsyncAppSettings,
            test_setting: Optional[TestAppSettings] = None,
    ) -> EnvSetupResult:
        auth_mw_list: list[AIOHTTPMiddleware]
        sr_middleware_list: list[AIOHTTPMiddleware]
        usm_middleware_list: list[AIOHTTPMiddleware]

        conn_opts_factory = ConnOptionsMutatorsFactory()
        sr_factory = self.get_sr_factory(settings=setting, conn_opts_factory=conn_opts_factory)

        # Auth middlewares
        yc_auth_settings = setting.YC_AUTH_SETTINGS
        assert yc_auth_settings
        yc_embed_auth_service = YCEmbedAuthService(
            authorization_mode=AuthorizationModeYandexCloud(
                folder_permission_to_check=yc_auth_settings.YC_AUTHORIZE_PERMISSION,
                organization_permission_to_check=yc_auth_settings.YC_AUTHORIZE_PERMISSION,
            ),
        )
        auth_mw_list = [yc_embed_auth_service.middleware]

        # SR middlewares

        def explain_select_conn_opts_mutator(
                conn_opts: ConnectOptions, conn: ExecutorBasedMixin
        ) -> Optional[ConnectOptions]:
            if setting.EXPLAIN_SELECT_FRAC:
                return conn_opts.clone(
                    explain_select_frac=setting.EXPLAIN_SELECT_FRAC,
                    explain_select_timeout=setting.EXPLAIN_SELECT_TIMEOUT,
                )
            return None

        conn_opts_factory.add_mutator(explain_select_conn_opts_mutator)

        def ignore_managed_conn_opts_mutator(
                conn_opts: ConnectOptions, conn: ExecutorBasedMixin
        ) -> Optional[ConnectOptions]:
            if setting.MDB_FORCE_IGNORE_MANAGED_NETWORK:
                return conn_opts.clone(use_managed_network=False)
            return None

        conn_opts_factory.add_mutator(ignore_managed_conn_opts_mutator)

        sr_middleware_list = [
            services_registry_middleware(
                services_registry_factory=sr_factory,
                use_query_cache=setting.CACHES_ON,
                use_mutation_cache=setting.MUTATIONS_CACHES_ON,
                mutation_cache_default_ttl=setting.MUTATIONS_CACHES_DEFAULT_TTL,
            ),
        ]

        # US manager middlewares
        common_us_kw = dict(
            us_base_url=setting.US_BASE_URL,
            crypto_keys_config=setting.CRYPTO_KEYS_CONFIG,
        )
        usm_middleware_list = [
            us_manager_middleware(embed=True, **common_us_kw),  # type: ignore
            service_us_manager_middleware(us_master_token=setting.US_MASTER_TOKEN, **common_us_kw) # type: ignore
        ]

        result = EnvSetupResult(
            auth_mw_list=auth_mw_list,
            sr_middleware_list=sr_middleware_list,
            usm_middleware_list=usm_middleware_list,
        )

        return result
