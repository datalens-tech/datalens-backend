from __future__ import annotations

from typing import Optional

from data_api import app_version

from bi_api_commons_ya_cloud.aio.middlewares.yc_auth import YCAuthService
from bi_api_commons_ya_cloud.constants import YcTokenHeaderMode
from bi_api_commons_ya_cloud.yc_access_control_model import AuthorizationModeYandexCloud
from bi_api_commons_ya_cloud.yc_auth import make_default_yc_auth_service_config
from bi_api_lib_ya.app_settings import (
    AsyncAppSettings,
    BaseAppSettings,
)
from bi_api_lib_ya.connections_security.base import MDBDomainManagerSettings
from bi_api_lib_ya.services_registry.env_manager_factory import CloudEnvManagerFactory
from bi_cloud_integration.sa_creds import (
    SACredsRetrieverFactory,
    SACredsSettings,
)
from bi_service_registry_nebius.nebius_service_registry import NebiusServiceRegistryFactory
from dl_api_commons.aio.typing import AIOHTTPMiddleware
from dl_api_lib.app.data_api.app import (
    DataApiAppFactory,
    EnvSetupResult,
)
from dl_api_lib.app_common import SRFactoryBuilder
from dl_api_lib.app_common_settings import ConnOptionsMutatorsFactory
from dl_api_lib.connector_availability.base import ConnectorAvailabilityConfig
from dl_configs.connectors_settings import ConnectorSettingsBase
from dl_configs.enums import (
    RQE_SERVICES,
    RequiredService,
)
from dl_constants.enums import ConnectionType
from dl_core.aio.middlewares.services_registry import services_registry_middleware
from dl_core.aio.middlewares.us_manager import (
    service_us_manager_middleware,
    us_manager_middleware,
)
from dl_core.connection_models import ConnectOptions
from dl_core.data_processing.cache.primitives import CacheTTLConfig
from dl_core.services_registry.entity_checker import EntityUsageChecker
from dl_core.services_registry.env_manager_factory_base import EnvManagerFactory
from dl_core.services_registry.inst_specific_sr import InstallationSpecificServiceRegistryFactory
from dl_core.services_registry.rqe_caches import RQECachesSetting
from dl_core.us_connection_base import ExecutorBasedMixin

from bi_connector_yql.core.ydb.us_connection import YDBConnectOptions  # TODO: remove dependency on connector


class DataApiSRFactoryBuilderNebius(SRFactoryBuilder[BaseAppSettings]):
    def _get_required_services(self, settings: BaseAppSettings) -> set[RequiredService]:
        return set(RQE_SERVICES)

    def _get_env_manager_factory(self, settings: BaseAppSettings) -> EnvManagerFactory:
        mdb_domain_manager_settings = MDBDomainManagerSettings(
            managed_network_enabled=settings.MDB.MANAGED_NETWORK_ENABLED,
            mdb_domains=settings.MDB.DOMAINS,
            mdb_cname_domains=settings.MDB.CNAME_DOMAINS,
            renaming_map=settings.MDB.MANAGED_NETWORK_REMAP,
        )
        return CloudEnvManagerFactory(
            mdb_domain_manager_settings=mdb_domain_manager_settings,
            samples_ch_hosts=list(settings.SAMPLES_CH_HOSTS),
        )

    def _get_inst_specific_sr_factory(
        self,
        settings: BaseAppSettings,
    ) -> Optional[InstallationSpecificServiceRegistryFactory]:
        sa_creds_settings = (
            SACredsSettings(
                mode=settings.YC_SA_CREDS_MODE,
                env_key_data=settings.YC_SA_CREDS_KEY_DATA,
            )
            if settings.YC_SA_CREDS_MODE is not None
            else None
        )

        return NebiusServiceRegistryFactory(
            yc_billing_host=settings.YC_BILLING_HOST,
            yc_api_endpoint_rm=settings.YC_RM_CP_ENDPOINT,
            yc_as_endpoint=settings.YC_AUTH_SETTINGS.YC_AS_ENDPOINT if settings.YC_AUTH_SETTINGS else None,
            yc_api_endpoint_iam=settings.YC_AUTH_SETTINGS.YC_API_ENDPOINT_IAM if settings.YC_AUTH_SETTINGS else None,
            yc_ts_endpoint=settings.YC_IAM_TS_ENDPOINT,
            sa_creds_retriever_factory=SACredsRetrieverFactory(
                sa_creds_settings=sa_creds_settings, ts_endpoint=settings.YC_IAM_TS_ENDPOINT
            )
            if sa_creds_settings
            else None,
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


class DataApiAppFactoryNebius(DataApiAppFactory[AsyncAppSettings], DataApiSRFactoryBuilderNebius):
    @property
    def _is_public(self) -> bool:
        return False

    def get_app_version(self) -> str:
        return app_version

    def set_up_environment(
        self,
        connectors_settings: dict[ConnectionType, ConnectorSettingsBase],
    ) -> EnvSetupResult:
        auth_mw_list: list[AIOHTTPMiddleware]
        sr_middleware_list: list[AIOHTTPMiddleware]
        usm_middleware_list: list[AIOHTTPMiddleware]

        conn_opts_factory = ConnOptionsMutatorsFactory()
        sr_factory = self.get_sr_factory(
            settings=self._settings, conn_opts_factory=conn_opts_factory, connectors_settings=connectors_settings
        )

        # Auth middlewares
        yc_auth_settings = self._settings.YC_AUTH_SETTINGS
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

        # SR middlewares

        def ydb_is_cloud_mutator(conn_opts: ConnectOptions, conn: ExecutorBasedMixin) -> Optional[ConnectOptions]:
            if isinstance(conn_opts, YDBConnectOptions):
                return conn_opts.clone(is_cloud=True)
            return None

        conn_opts_factory.add_mutator(ydb_is_cloud_mutator)

        sr_middleware_list = [
            services_registry_middleware(
                services_registry_factory=sr_factory,
                use_query_cache=self._settings.CACHES_ON,
                use_mutation_cache=self._settings.MUTATIONS_CACHES_ON,
                mutation_cache_default_ttl=self._settings.MUTATIONS_CACHES_DEFAULT_TTL,
            ),
        ]

        # US manager middlewares
        common_us_kw = dict(
            us_base_url=self._settings.US_BASE_URL,
            crypto_keys_config=self._settings.CRYPTO_KEYS_CONFIG,
        )
        usm_middleware_list = [
            us_manager_middleware(**common_us_kw),  # type: ignore
            service_us_manager_middleware(us_master_token=self._settings.US_MASTER_TOKEN, **common_us_kw),  # type: ignore
        ]

        result = EnvSetupResult(
            auth_mw_list=auth_mw_list,
            sr_middleware_list=sr_middleware_list,
            usm_middleware_list=usm_middleware_list,
        )

        return result
