from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Optional,
)

import attr

from dl_api_lib.app_common import SRFactoryBuilder
from dl_api_lib.app_common_settings import ConnOptionsMutatorsFactory
from dl_api_lib.app_settings import AppSettings
from dl_api_lib.loader import (
    ApiLibraryConfig,
    load_api_lib,
    preload_api_lib,
)
from dl_configs.settings_loaders.loader_env import load_settings_from_env_with_fallback
from dl_core.loader import CoreLibraryConfig
from dl_maintenance.core.common import MaintenanceEnvironmentManagerBase


if TYPE_CHECKING:
    from dl_core.connectors.settings.base import ConnectorSettings
    from dl_core.services_registry.sr_factories import SRFactory


@attr.s(kw_only=True)
class MaintenanceEnvironmentManager(MaintenanceEnvironmentManagerBase):
    _app_settings_cls: type[AppSettings] = attr.ib()
    _app_factory_cls: Optional[type[SRFactoryBuilder]] = attr.ib(default=None)

    def get_app_settings(self) -> AppSettings:
        preload_api_lib()
        settings = load_settings_from_env_with_fallback(self._app_settings_cls)
        load_api_lib(
            ApiLibraryConfig(
                api_connector_ep_names=settings.BI_API_CONNECTOR_WHITELIST,
                core_lib_config=CoreLibraryConfig(core_connector_ep_names=settings.CORE_CONNECTOR_WHITELIST),
            )
        )
        return settings

    def get_connector_settings(self) -> dict[str, ConnectorSettings]:
        return {}

    def get_sr_factory(self, ca_data: bytes, is_async_env: bool) -> Optional[SRFactory]:
        assert self._app_factory_cls is not None
        conn_opts_factory = ConnOptionsMutatorsFactory()
        settings = self.get_app_settings()
        sr_factory = self._app_factory_cls(settings=settings).get_sr_factory(  # type: ignore  # 2024-01-30 # TODO: Unexpected keyword argument "settings" for "SRFactoryBuilder"  [call-arg]
            settings=settings,
            conn_opts_factory=conn_opts_factory,
            connectors_settings=self.get_connector_settings(),
            ca_data=ca_data,
        )
        return sr_factory
