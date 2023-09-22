from __future__ import annotations

import abc
from typing import Optional

from bi_api_lib_ya.app_settings import (
    AsyncAppSettings,
    BaseAppSettings,
    ControlPlaneAppSettings,
)
from bi_api_lib_ya.connections_security.base import MDBDomainManagerSettings
from bi_api_lib_ya.i18n.localizer import CONFIGS
from bi_api_lib_ya.services_registry.env_manager_factory import (
    CloudEnvManagerFactory,
    DataCloudEnvManagerFactory,
    IntranetEnvManagerFactory,
)
from bi_cloud_integration.sa_creds import (
    SACredsRetrieverFactory,
    SACredsSettings,
)
from bi_defaults.yenv_type import AppType
from bi_service_registry_ya_cloud.yc_service_registry import YCServiceRegistryFactory
from bi_service_registry_ya_team.yt_service_registry import YTServiceRegistryFactory
from dl_api_lib.app_common import SRFactoryBuilder
from dl_api_lib.connector_availability.base import ConnectorAvailabilityConfig
from dl_api_lib.public.entity_usage_checker import PublicEnvEntityUsageChecker
from dl_configs.enums import (
    RQE_SERVICES,
    RequiredService,
)
from dl_core.data_processing.cache.primitives import CacheTTLConfig
from dl_core.services_registry.entity_checker import EntityUsageChecker
from dl_core.services_registry.env_manager_factory_base import EnvManagerFactory
from dl_core.services_registry.inst_specific_sr import InstallationSpecificServiceRegistryFactory
from dl_core.services_registry.rqe_caches import RQECachesSetting
from dl_core_testing.app_test_workarounds import TestEnvManagerFactory
from dl_i18n.localizer_base import TranslationConfig


class LegacySRFactoryBuilder(SRFactoryBuilder[BaseAppSettings], abc.ABC):
    def _get_required_services(self, settings: BaseAppSettings) -> set[RequiredService]:
        required_services: set[RequiredService] = {  # type: ignore
            AppType.CLOUD: RQE_SERVICES,
            AppType.CLOUD_PUBLIC: RQE_SERVICES,
            AppType.CLOUD_EMBED: RQE_SERVICES,
            AppType.INTRANET: {RequiredService.RQE_INT_SYNC},
            AppType.TESTS: {RequiredService.RQE_INT_SYNC, RequiredService.RQE_EXT_SYNC},
            AppType.DATA_CLOUD: set(),
            AppType.NEBIUS: RQE_SERVICES,
        }.get(settings.APP_TYPE, set())
        return required_services

    def _get_env_manager_factory(self, settings: BaseAppSettings) -> EnvManagerFactory:
        if settings.APP_TYPE == AppType.INTRANET:
            mdb_domain_manager_settings = MDBDomainManagerSettings(
                managed_network_enabled=settings.MDB.MANAGED_NETWORK_ENABLED,
                mdb_domains=settings.MDB.DOMAINS,
                mdb_cname_domains=settings.MDB.CNAME_DOMAINS,
                renaming_map=settings.MDB.MANAGED_NETWORK_REMAP,
            )
            return IntranetEnvManagerFactory(mdb_domain_manager_settings=mdb_domain_manager_settings)
        elif settings.APP_TYPE == AppType.TESTS:
            # TODO FIX: BI-2517 switch to normal DataCloud mode
            return TestEnvManagerFactory()
        elif settings.APP_TYPE == AppType.DATA_CLOUD:
            mdb_domain_manager_settings = MDBDomainManagerSettings(
                managed_network_enabled=settings.MDB.MANAGED_NETWORK_ENABLED,
                mdb_domains=settings.MDB.DOMAINS,
                mdb_cname_domains=settings.MDB.CNAME_DOMAINS,
                renaming_map=settings.MDB.MANAGED_NETWORK_REMAP,
            )
            return DataCloudEnvManagerFactory(
                mdb_domain_manager_settings=mdb_domain_manager_settings,
                samples_ch_hosts=list(settings.SAMPLES_CH_HOSTS),
            )
        else:
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
        if settings.APP_TYPE == AppType.INTRANET:
            return YTServiceRegistryFactory(
                dls_host=settings.DLS_HOST,
                dls_api_key=settings.DLS_API_KEY,
            )
        else:
            sa_creds_settings = (
                SACredsSettings(
                    mode=settings.YC_SA_CREDS_MODE,
                    env_key_data=settings.YC_SA_CREDS_KEY_DATA,
                )
                if settings.YC_SA_CREDS_MODE is not None
                else None
            )

            return YCServiceRegistryFactory(
                yc_billing_host=settings.YC_BILLING_HOST if settings.APP_TYPE == AppType.CLOUD else None,
                yc_api_endpoint_rm=settings.YC_RM_CP_ENDPOINT,
                yc_as_endpoint=settings.YC_AUTH_SETTINGS.YC_AS_ENDPOINT if settings.YC_AUTH_SETTINGS else None,
                yc_api_endpoint_iam=settings.YC_AUTH_SETTINGS.YC_API_ENDPOINT_IAM
                if settings.YC_AUTH_SETTINGS
                else None,
                yc_ts_endpoint=settings.YC_IAM_TS_ENDPOINT,
                yc_api_endpoint_mdb=settings.YC_MDB_API_ENDPOINT,
                sa_creds_retriever_factory=SACredsRetrieverFactory(
                    sa_creds_settings=sa_creds_settings, ts_endpoint=settings.YC_IAM_TS_ENDPOINT
                )
                if sa_creds_settings
                else None,
                blackbox_name=settings.BLACKBOX_NAME,
            )

    def _get_entity_usage_checker(self, settings: BaseAppSettings) -> Optional[EntityUsageChecker]:
        if settings.APP_TYPE == AppType.CLOUD_PUBLIC:
            return PublicEnvEntityUsageChecker()
        return None

    def _get_bleeding_edge_users(self, settings: BaseAppSettings) -> tuple[str, ...]:
        return settings.BLEEDING_EDGE_USERS if settings.APP_TYPE in (AppType.INTRANET, AppType.TESTS) else tuple()

    def _get_rqe_caches_settings(self, settings: BaseAppSettings) -> Optional[RQECachesSetting]:
        if (
            settings.APP_TYPE in (AppType.CLOUD, AppType.TESTS)
            and isinstance(settings, AsyncAppSettings)
            and settings.RQE_CACHES_ON
        ):
            return RQECachesSetting(
                redis_settings=settings.RQE_CACHES_REDIS,
                caches_ttl=settings.RQE_CACHES_TTL,
            )
        return None

    def _get_default_cache_ttl_settings(self, settings: BaseAppSettings) -> Optional[CacheTTLConfig]:
        if isinstance(settings, AsyncAppSettings) and settings.CACHES_TTL_SETTINGS is not None:
            # FIXME: resolve typing mismatch for CacheTTLConfig
            return CacheTTLConfig(
                ttl_sec_direct=settings.CACHES_TTL_SETTINGS.OTHER,  # type: ignore  # TODO: fix
                ttl_sec_materialized=settings.CACHES_TTL_SETTINGS.MATERIALIZED,  # type: ignore  # TODO: fix
            )
        return None

    def _get_connector_availability(self, settings: BaseAppSettings) -> Optional[ConnectorAvailabilityConfig]:
        return settings.CONNECTOR_AVAILABILITY if isinstance(settings, ControlPlaneAppSettings) else None

    @property
    def _extra_translation_configs(self) -> set[TranslationConfig]:
        configs = super()._extra_translation_configs
        configs.update(CONFIGS)
        return configs
