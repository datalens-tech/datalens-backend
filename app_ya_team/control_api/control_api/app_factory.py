from __future__ import annotations

from typing import Optional

from control_api.i18n.localizer import CONFIGS
import flask

from bi_api_commons_ya_team.flask.middlewares import blackbox_auth
from bi_api_lib_ya.app_settings import ControlApiAppSettingsYT
from bi_api_lib_ya.connections_security.base import MDBDomainManagerSettings
from bi_api_lib_ya.services_registry.env_manager_factory import IntranetEnvManagerFactory
from bi_service_registry_ya_team.yt_service_registry import YTServiceRegistryFactory
from dl_api_lib.app.control_api.app import (
    ControlApiAppFactory,
    EnvSetupResult,
)
from dl_api_lib.app_common import SRFactoryBuilder
from dl_api_lib.app_settings import ControlApiAppTestingsSettings
from dl_api_lib.connector_availability.base import ConnectorAvailabilityConfig
from dl_configs.enums import RequiredService
from dl_constants.enums import USAuthMode
from dl_core.data_processing.cache.primitives import CacheTTLConfig
from dl_core.services_registry.entity_checker import EntityUsageChecker
from dl_core.services_registry.env_manager_factory_base import EnvManagerFactory
from dl_core.services_registry.inst_specific_sr import InstallationSpecificServiceRegistryFactory
from dl_core.services_registry.rqe_caches import RQECachesSetting
from dl_i18n.localizer_base import TranslationConfig


class ControlApiSRFactoryBuilderYT(SRFactoryBuilder[ControlApiAppSettingsYT]):
    def _get_required_services(self, settings: ControlApiAppSettingsYT) -> set[RequiredService]:
        required_services: set[RequiredService] = {
            RequiredService.RQE_INT_SYNC,
        }
        return required_services

    def _get_env_manager_factory(self, settings: ControlApiAppSettingsYT) -> EnvManagerFactory:
        mdb_domain_manager_settings = MDBDomainManagerSettings(
            managed_network_enabled=settings.MDB.MANAGED_NETWORK_ENABLED,
            mdb_domains=settings.MDB.DOMAINS,
            mdb_cname_domains=settings.MDB.CNAME_DOMAINS,
            renaming_map=settings.MDB.MANAGED_NETWORK_REMAP,
        )
        return IntranetEnvManagerFactory(mdb_domain_manager_settings=mdb_domain_manager_settings)

    def _get_inst_specific_sr_factory(
        self,
        settings: ControlApiAppSettingsYT,
    ) -> Optional[InstallationSpecificServiceRegistryFactory]:
        return YTServiceRegistryFactory(
            dls_host=settings.DLS_HOST,
            dls_api_key=settings.DLS_API_KEY,
        )

    def _get_entity_usage_checker(self, settings: ControlApiAppSettingsYT) -> Optional[EntityUsageChecker]:
        return None

    def _get_bleeding_edge_users(self, settings: ControlApiAppSettingsYT) -> tuple[str, ...]:
        return settings.BLEEDING_EDGE_USERS

    def _get_rqe_caches_settings(self, settings: ControlApiAppSettingsYT) -> Optional[RQECachesSetting]:
        return None

    def _get_default_cache_ttl_settings(self, settings: ControlApiAppSettingsYT) -> Optional[CacheTTLConfig]:
        return None

    def _get_connector_availability(self, settings: ControlApiAppSettingsYT) -> Optional[ConnectorAvailabilityConfig]:
        return settings.CONNECTOR_AVAILABILITY

    @property
    def _extra_translation_configs(self) -> set[TranslationConfig]:
        configs = super()._extra_translation_configs
        configs.update(CONFIGS)
        return configs


class ControlApiAppFactoryYT(ControlApiAppFactory[ControlApiAppSettingsYT], ControlApiSRFactoryBuilderYT):
    def set_up_environment(
        self,
        app: flask.Flask,
        testing_app_settings: Optional[ControlApiAppTestingsSettings] = None,
    ) -> EnvSetupResult:
        us_auth_mode: USAuthMode

        blackbox_auth.set_up(app, self._settings.BLACKBOX_RETRY_PARAMS, self._settings.BLACKBOX_TIMEOUT)
        us_auth_mode = USAuthMode.regular
        result = EnvSetupResult(us_auth_mode=us_auth_mode)
        return result
