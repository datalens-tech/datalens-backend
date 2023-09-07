import abc
from typing import Optional

from bi_configs.enums import RequiredService

from bi_core.data_processing.cache.primitives import CacheTTLConfig
from bi_core.services_registry.entity_checker import EntityUsageChecker
from bi_core.services_registry.env_manager_factory_base import EnvManagerFactory
from bi_core.services_registry.inst_specific_sr import InstallationSpecificServiceRegistryFactory
from bi_core.services_registry.rqe_caches import RQECachesSetting
from bi_core_testing.app_test_workarounds import TestEnvManagerFactory

from bi_cloud_integration.sa_creds import SACredsSettings, SACredsRetrieverFactory
from bi_service_registry_ya_cloud.yc_service_registry import YCServiceRegistryFactory

from bi_api_lib.app.control_api.app import ControlApiAppFactory
from bi_api_lib.app.data_api.app import DataApiAppFactory
from bi_api_lib.app_common import SRFactoryBuilder
from bi_api_lib.app_settings import BaseAppSettings, AsyncAppSettings, ControlPlaneAppSettings
from bi_api_lib.connector_availability.base import ConnectorAvailabilityConfig


class PrivateTestingSRFactoryBuilder(SRFactoryBuilder[BaseAppSettings], abc.ABC):
    def _get_required_services(self, settings: BaseAppSettings) -> set[RequiredService]:
        return {RequiredService.RQE_INT_SYNC, RequiredService.RQE_EXT_SYNC}

    def _get_env_manager_factory(self, settings: BaseAppSettings) -> EnvManagerFactory:
        return TestEnvManagerFactory()

    def _get_inst_specific_sr_factory(
            self,
            settings: BaseAppSettings,
    ) -> Optional[InstallationSpecificServiceRegistryFactory]:
        sa_creds_settings = SACredsSettings(
            mode=settings.YC_SA_CREDS_MODE,
            env_key_data=settings.YC_SA_CREDS_KEY_DATA,
        ) if settings.YC_SA_CREDS_MODE is not None else None

        return YCServiceRegistryFactory(
            yc_billing_host=None,
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
        return settings.BLEEDING_EDGE_USERS

    def _get_rqe_caches_settings(self, settings: BaseAppSettings) -> Optional[RQECachesSetting]:
        if isinstance(settings, AsyncAppSettings) and settings.RQE_CACHES_ON:
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


class TestingControlApiAppFactoryPrivate(ControlApiAppFactory, PrivateTestingSRFactoryBuilder):
    """Management API app factory for tests"""


class TestingDataApiAppFactoryPrivate(DataApiAppFactory, PrivateTestingSRFactoryBuilder):
    """Data API app factory for tests"""

    def get_app_version(self) -> str:
        return 'tests'
