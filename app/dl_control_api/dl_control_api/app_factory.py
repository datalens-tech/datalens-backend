from __future__ import annotations

from typing import Optional

import flask

from dl_api_lib.app.control_api.app import (
    ControlApiAppFactory,
    EnvSetupResult,
)
from dl_api_lib.app_common import (
    SRFactoryBuilder,
    StandaloneServiceRegistryFactory,
)
from dl_api_lib.app_settings import (
    ControlApiAppSettings,
    ControlApiAppTestingsSettings,
)
from dl_api_lib.connector_availability.base import ConnectorAvailabilityConfig
from dl_configs.enums import RequiredService
from dl_constants.enums import USAuthMode
from dl_core.data_processing.cache.primitives import CacheTTLConfig
from dl_core.services_registry.entity_checker import EntityUsageChecker
from dl_core.services_registry.env_manager_factory import InsecureEnvManagerFactory
from dl_core.services_registry.env_manager_factory_base import EnvManagerFactory
from dl_core.services_registry.rqe_caches import RQECachesSetting


class StandaloneControlApiSRFactoryBuilder(SRFactoryBuilder[ControlApiAppSettings]):
    def _get_required_services(self, settings: ControlApiAppSettings) -> set[RequiredService]:
        return set()

    def _get_env_manager_factory(self, settings: ControlApiAppSettings) -> EnvManagerFactory:
        return InsecureEnvManagerFactory()

    def _get_inst_specific_sr_factory(
        self,
        settings: ControlApiAppSettings,
    ) -> StandaloneServiceRegistryFactory:
        return StandaloneServiceRegistryFactory()

    def _get_entity_usage_checker(self, settings: ControlApiAppSettings) -> Optional[EntityUsageChecker]:
        return None

    def _get_bleeding_edge_users(self, settings: ControlApiAppSettings) -> tuple[str, ...]:
        return tuple()

    def _get_rqe_caches_settings(self, settings: ControlApiAppSettings) -> Optional[RQECachesSetting]:
        return None

    def _get_default_cache_ttl_settings(self, settings: ControlApiAppSettings) -> Optional[CacheTTLConfig]:
        return None

    def _get_connector_availability(self, settings: ControlApiAppSettings) -> Optional[ConnectorAvailabilityConfig]:
        return settings.CONNECTOR_AVAILABILITY


class StandaloneControlApiAppFactory(ControlApiAppFactory[ControlApiAppSettings], StandaloneControlApiSRFactoryBuilder):
    def set_up_environment(
        self,
        app: flask.Flask,
        testing_app_settings: Optional[ControlApiAppTestingsSettings] = None,
    ) -> EnvSetupResult:
        us_auth_mode: USAuthMode
        from dl_api_commons.flask.middlewares.trust_auth import TrustAuthService

        TrustAuthService(
            fake_user_id="_user_id_",
            fake_user_name="_user_name_",
            fake_tenant=None if testing_app_settings is None else testing_app_settings.fake_tenant,
        ).set_up(app)

        us_auth_mode_override = None if testing_app_settings is None else testing_app_settings.us_auth_mode_override
        us_auth_mode = USAuthMode.master if us_auth_mode_override is None else us_auth_mode_override

        result = EnvSetupResult(us_auth_mode=us_auth_mode)
        return result
