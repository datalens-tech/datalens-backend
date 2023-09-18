import abc
from typing import Optional

from bi_api_lib_ya.app.control_api.app import LegacyControlApiAppFactory
from bi_api_lib_ya.app.data_api.app import LegacyDataApiAppFactory
from bi_api_lib_ya.app_common import LegacySRFactoryBuilder
from bi_api_lib_ya.app_settings import (
    AsyncAppSettings,
    BaseAppSettings,
)
from bi_cloud_integration.sa_creds import (
    SACredsRetrieverFactory,
    SACredsSettings,
)
from bi_service_registry_ya_cloud.yc_service_registry import YCServiceRegistryFactory
from dl_configs.enums import RequiredService
from dl_core.services_registry.entity_checker import EntityUsageChecker
from dl_core.services_registry.env_manager_factory_base import EnvManagerFactory
from dl_core.services_registry.inst_specific_sr import InstallationSpecificServiceRegistryFactory
from dl_core.services_registry.rqe_caches import RQECachesSetting
from dl_core_testing.app_test_workarounds import TestEnvManagerFactory


class PrivateTestingSRFactoryBuilder(LegacySRFactoryBuilder, abc.ABC):
    def _get_required_services(self, settings: BaseAppSettings) -> set[RequiredService]:
        return {RequiredService.RQE_INT_SYNC, RequiredService.RQE_EXT_SYNC}

    def _get_env_manager_factory(self, settings: BaseAppSettings) -> EnvManagerFactory:
        return TestEnvManagerFactory()

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

        return YCServiceRegistryFactory(
            yc_billing_host=None,
            yc_api_endpoint_rm=settings.YC_RM_CP_ENDPOINT,
            yc_as_endpoint=settings.YC_AUTH_SETTINGS.YC_AS_ENDPOINT if settings.YC_AUTH_SETTINGS else None,
            yc_api_endpoint_iam=settings.YC_AUTH_SETTINGS.YC_API_ENDPOINT_IAM if settings.YC_AUTH_SETTINGS else None,
            yc_ts_endpoint=settings.YC_IAM_TS_ENDPOINT,
            sa_creds_retriever_factory=SACredsRetrieverFactory(
                sa_creds_settings=sa_creds_settings, ts_endpoint=settings.YC_IAM_TS_ENDPOINT
            )
            if sa_creds_settings
            else None,
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


class TestingControlApiAppFactoryPrivate(LegacyControlApiAppFactory, PrivateTestingSRFactoryBuilder):
    """Management API app factory for tests"""


class TestingDataApiAppFactoryPrivate(LegacyDataApiAppFactory, PrivateTestingSRFactoryBuilder):
    """Data API app factory for tests"""

    def get_app_version(self) -> str:
        return "tests"
