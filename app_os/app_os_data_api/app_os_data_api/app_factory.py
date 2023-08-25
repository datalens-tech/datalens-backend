from __future__ import annotations

from typing import Optional

from bi_configs.enums import RequiredService

from bi_core.aio.middlewares.auth_trust_middleware import auth_trust_middleware
from bi_core.aio.middlewares.services_registry import services_registry_middleware
from bi_core.aio.middlewares.us_manager import service_us_manager_middleware
from bi_core.connection_models import ConnectOptions
from bi_core.data_processing.cache.primitives import CacheTTLConfig
from bi_core.services_registry.entity_checker import EntityUsageChecker
from bi_core.services_registry.env_manager_factory_base import EnvManagerFactory
from bi_core.services_registry.inst_specific_sr import InstallationSpecificServiceRegistryFactory
from bi_core.services_registry.rqe_caches import RQECachesSetting
from bi_core.us_connection_base import ExecutorBasedMixin

from bi_api_lib.app_common import SRFactoryBuilder
from bi_api_lib.app_common_settings import ConnOptionsMutatorsFactory
from bi_api_lib.app.data_api.app import DataApiAppFactory, EnvSetupResult
from bi_api_lib.app_settings import BaseAppSettings, AsyncAppSettings, TestAppSettings
from bi_api_lib.connector_availability.base import ConnectorAvailabilityConfig

from bi_core_testing.app_test_workarounds import TestEnvManagerFactory
from bi_api_commons.aio.typing import AIOHTTPMiddleware

from app_os_data_api import app_version


class DataApiSRFactoryBuilderOS(SRFactoryBuilder):
    def _get_required_services(self, settings: BaseAppSettings) -> set[RequiredService]:
        return set()

    def _get_env_manager_factory(self, settings: BaseAppSettings) -> EnvManagerFactory:
        return TestEnvManagerFactory()

    def _get_inst_specific_sr_factory(
            self,
            settings: BaseAppSettings,
    ) -> Optional[InstallationSpecificServiceRegistryFactory]:
        return None

    def _get_entity_usage_checker(self, settings: BaseAppSettings) -> Optional[EntityUsageChecker]:
        return None

    def _get_bleeding_edge_users(self, settings: BaseAppSettings) -> tuple[str, ...]:
        return tuple()

    def _get_rqe_caches_settings(self, settings: AsyncAppSettings) -> Optional[RQECachesSetting]:  # type: ignore[override]
        return None

    def _get_default_cache_ttl_settings(self, settings: AsyncAppSettings) -> Optional[CacheTTLConfig]:  # type: ignore[override]
        return None

    def _get_connector_availability(self, settings: BaseAppSettings) -> Optional[ConnectorAvailabilityConfig]:
        return None


class DataApiAppFactoryOS(DataApiAppFactory, DataApiSRFactoryBuilderOS):
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
        auth_mw_list = [
            auth_trust_middleware(
                fake_user_id='_user_id_',
                fake_user_name='_user_name_',
            )
        ]

        # SR middlewares
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
            service_us_manager_middleware(us_master_token=setting.US_MASTER_TOKEN, **common_us_kw),  # type: ignore
            service_us_manager_middleware(us_master_token=setting.US_MASTER_TOKEN, as_user_usm=True, **common_us_kw),  # type: ignore
        ]

        result = EnvSetupResult(
            auth_mw_list=auth_mw_list,
            sr_middleware_list=sr_middleware_list,
            usm_middleware_list=usm_middleware_list,
        )

        return result
