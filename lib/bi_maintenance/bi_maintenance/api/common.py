from __future__ import annotations

from typing import Optional, TYPE_CHECKING

from bi_api_lib.app_common import LegacySRFactoryBuilder
from bi_api_lib.app_common_settings import ConnOptionsMutatorsFactory
from bi_api_lib.app_settings import AsyncAppSettings
from bi_api_lib.app.data_api.app import DataApiAppFactory
from bi_api_lib.loader import ApiLibraryConfig, load_bi_api_lib
from bi_configs.settings_loaders.loader_env import (
    load_settings_from_env_with_fallback, load_connectors_settings_from_env_with_fallback,
)
from bi_core.connectors.settings.registry import CONNECTORS_SETTINGS_CLASSES, CONNECTORS_SETTINGS_FALLBACKS
from bi_maintenance.core.common import MaintenanceEnvironmentManagerBase

if TYPE_CHECKING:
    from bi_core.services_registry.sr_factories import SRFactory


class MaintenanceDataApiAppFactory(DataApiAppFactory, LegacySRFactoryBuilder):
    def get_app_version(self) -> str:
        return ''


class MaintenanceEnvironmentManager(MaintenanceEnvironmentManagerBase):
    def get_app_settings(self) -> AsyncAppSettings:
        settings = load_settings_from_env_with_fallback(AsyncAppSettings)
        load_bi_api_lib(ApiLibraryConfig(api_connector_ep_names=settings.CONNECTOR_WHITELIST))
        return settings

    def get_sr_factory(self, is_async_env: bool) -> Optional[SRFactory]:
        conn_opts_factory = ConnOptionsMutatorsFactory()
        settings = self.get_app_settings()
        sr_factory = MaintenanceDataApiAppFactory(settings=settings).get_sr_factory(
            settings=settings,
            conn_opts_factory=conn_opts_factory,
            connectors_settings=load_connectors_settings_from_env_with_fallback(
                settings_registry=CONNECTORS_SETTINGS_CLASSES,
                fallbacks=CONNECTORS_SETTINGS_FALLBACKS,
            ),
        )
        return sr_factory
