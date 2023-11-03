from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Any,
    Optional,
)

import attr

from dl_api_commons.base_models import RequestContextInfo
from dl_core.united_storage_client import USAuthContextMaster
from dl_core.us_manager.us_manager_async import AsyncUSManager
from dl_core.us_manager.us_manager_sync import SyncUSManager


if TYPE_CHECKING:
    from dl_core.services_registry.sr_factories import SRFactory


@attr.s
class UsConfig:
    base_url: str = attr.ib(kw_only=True)
    master_token: str = attr.ib(kw_only=True)


class MaintenanceEnvironmentManagerBase:
    def get_app_settings(self) -> Any:
        raise NotImplementedError

    def get_us_config(self) -> UsConfig:
        app_settings = self.get_app_settings()
        return UsConfig(
            base_url=app_settings.US_BASE_URL,
            master_token=app_settings.US_MASTER_TOKEN,
        )

    def get_sr_factory(self, is_async_env: bool) -> Optional[SRFactory]:
        return None

    def get_usm_from_env(self, use_sr_factory: bool = True, is_async_env: bool = True) -> SyncUSManager:
        us_config = self.get_us_config()
        rci = RequestContextInfo.create_empty()
        sr_factory = self.get_sr_factory(is_async_env=is_async_env) if use_sr_factory else None
        service_registry = sr_factory.make_service_registry(rci) if sr_factory is not None else None

        return SyncUSManager(
            us_base_url=us_config.base_url,
            us_auth_context=USAuthContextMaster(us_master_token=us_config.master_token),
            bi_context=rci,
            services_registry=service_registry,
        )

    def get_async_usm_from_env(self, use_sr_factory: bool = True) -> AsyncUSManager:
        us_config = self.get_us_config()
        rci = RequestContextInfo.create_empty()
        sr_factory = self.get_sr_factory(is_async_env=True) if use_sr_factory else None
        service_registry = sr_factory.make_service_registry(rci) if sr_factory is not None else None

        return AsyncUSManager(
            us_base_url=us_config.base_url,
            us_auth_context=USAuthContextMaster(us_master_token=us_config.master_token),
            bi_context=rci,
            services_registry=service_registry,
        )
