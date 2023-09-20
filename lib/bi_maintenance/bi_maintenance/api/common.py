from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Optional,
)

from bi_api_lib_ya.app.data_api.app import LegacyDataApiAppFactory
from bi_api_lib_ya.app_common import LegacySRFactoryBuilder
from bi_api_lib_ya.app_settings import AsyncAppSettings
from bi_defaults.environments import (
    EnvAliasesMap,
    InstallationsMap,
)
from bi_defaults.yenv_type import YEnvFallbackConfigResolver
from bi_maintenance.core.common import MaintenanceEnvironmentManagerBase
from dl_api_lib.app_common_settings import ConnOptionsMutatorsFactory
from dl_api_lib.loader import (
    ApiLibraryConfig,
    load_bi_api_lib,
    preload_bi_api_lib,
)
from dl_configs.settings_loaders.loader_env import (
    load_connectors_settings_from_env_with_fallback,
    load_settings_from_env_with_fallback,
)
from dl_core.connectors.settings.registry import (
    CONNECTORS_SETTINGS_CLASSES,
    CONNECTORS_SETTINGS_FALLBACKS,
)
from dl_core.loader import CoreLibraryConfig


if TYPE_CHECKING:
    from dl_core.services_registry.sr_factories import SRFactory


class MaintenanceDataApiAppFactory(LegacyDataApiAppFactory, LegacySRFactoryBuilder):
    def get_app_version(self) -> str:
        return ""


class MaintenanceEnvironmentManager(MaintenanceEnvironmentManagerBase):
    def get_app_settings(self) -> AsyncAppSettings:
        preload_bi_api_lib()
        fallback_resolver = YEnvFallbackConfigResolver(
            installation_map=InstallationsMap,
            env_map=EnvAliasesMap,
        )
        settings = load_settings_from_env_with_fallback(
            AsyncAppSettings,
            default_fallback_cfg_resolver=fallback_resolver,
        )
        load_bi_api_lib(
            ApiLibraryConfig(
                api_connector_ep_names=settings.BI_API_CONNECTOR_WHITELIST,
                core_lib_config=CoreLibraryConfig(core_connector_ep_names=settings.CORE_CONNECTOR_WHITELIST),
            )
        )
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
