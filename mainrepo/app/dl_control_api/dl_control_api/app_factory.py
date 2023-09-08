from __future__ import annotations

from typing import Optional

import flask

from bi_configs.enums import RequiredService
from bi_constants.enums import USAuthMode

from bi_api_lib.app.control_api.app import EnvSetupResult, ControlApiAppFactory
from bi_api_lib.app_common import SRFactoryBuilder, StandaloneServiceRegistryFactory
from bi_api_lib.app_settings import ControlApiAppTestingsSettings, ControlApiAppSettings
from bi_api_lib.connector_availability.base import ConnectorAvailabilityConfig

from bi_core.data_processing.cache.primitives import CacheTTLConfig
from bi_core.services_registry.entity_checker import EntityUsageChecker
from bi_core.services_registry.env_manager_factory_base import EnvManagerFactory
from bi_core.services_registry.rqe_caches import RQECachesSetting
from bi_core_testing.app_test_workarounds import TestEnvManagerFactory


class ControlApiSRFactoryBuilderOS(SRFactoryBuilder[ControlApiAppSettings]):
    def _get_required_services(self, settings: ControlApiAppSettings) -> set[RequiredService]:
        return set()

    def _get_env_manager_factory(self, settings: ControlApiAppSettings) -> EnvManagerFactory:
        return TestEnvManagerFactory()

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


class ControlApiAppFactoryOS(ControlApiAppFactory[ControlApiAppSettings], ControlApiSRFactoryBuilderOS):
    def set_up_environment(
            self,
            app: flask.Flask,
            testing_app_settings: Optional[ControlApiAppTestingsSettings] = None,
    ) -> EnvSetupResult:
        us_auth_mode: USAuthMode
        from bi_core.flask_utils.trust_auth import TrustAuthService
        TrustAuthService(
            fake_user_id='_user_id_',
            fake_user_name='_user_name_',
            fake_tenant=None if testing_app_settings is None else testing_app_settings.fake_tenant
        ).set_up(app)

        us_auth_mode_override = None if testing_app_settings is None else testing_app_settings.us_auth_mode_override
        us_auth_mode = USAuthMode.master if us_auth_mode_override is None else us_auth_mode_override

        result = EnvSetupResult(us_auth_mode=us_auth_mode)
        return result
