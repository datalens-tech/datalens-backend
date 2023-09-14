from typing import Optional

import attr

from bi_api_commons.base_models import RequestContextInfo
from bi_configs.crypto_keys import CryptoKeysConfig
from bi_constants.api_constants import DLHeadersCommon
from bi_core.services_registry import ServicesRegistry
from bi_core.united_storage_client import (
    USAuthContextEmbed,
    USAuthContextMaster,
    USAuthContextPublic,
    USAuthContextRegular,
)
from bi_core.us_manager.us_manager_async import AsyncUSManager
from bi_core.us_manager.us_manager_sync import SyncUSManager


@attr.s(frozen=True)
class USMFactory:
    us_base_url: str = attr.ib()
    crypto_keys_config: Optional[CryptoKeysConfig] = attr.ib()
    us_master_token: Optional[str] = attr.ib(default=None, repr=False)
    us_public_token: Optional[str] = attr.ib(default=None, repr=False)

    @classmethod
    def get_regular_us_auth_ctx_from_rci(cls, rci: RequestContextInfo) -> USAuthContextRegular:
        auth_data = rci.auth_data
        tenant = rci.tenant
        assert tenant is not None
        assert auth_data is not None

        return USAuthContextRegular(
            auth_data=auth_data,
            tenant=tenant,
            dl_allow_super_user=rci.plain_headers.get(DLHeadersCommon.ALLOW_SUPERUSER.value),
            dl_sudo=rci.plain_headers.get(DLHeadersCommon.SUDO.value),
        )

    def def_embed_us_auth_ctx_from_rci(self, rci: RequestContextInfo) -> USAuthContextEmbed:
        auth_data = rci.auth_data
        tenant = rci.tenant
        assert tenant is not None
        assert auth_data is not None

        return USAuthContextEmbed(
            auth_data=auth_data,
            tenant=tenant,
        )

    def get_master_auth_context(self) -> USAuthContextMaster:
        assert self.us_master_token is not None, "US master token must be set in factory to create USAuthContextMaster"
        return USAuthContextMaster(us_master_token=self.us_master_token)

    # Async
    def get_regular_async_usm(
        self,
        rci: RequestContextInfo,
        services_registry: ServicesRegistry,
    ) -> AsyncUSManager:
        return AsyncUSManager(
            us_auth_context=self.get_regular_us_auth_ctx_from_rci(rci),
            us_base_url=self.us_base_url,
            bi_context=rci,
            crypto_keys_config=self.crypto_keys_config,
            services_registry=services_registry,
        )

    def get_master_async_usm(
        self,
        rci: RequestContextInfo,
        services_registry: ServicesRegistry,
    ) -> AsyncUSManager:
        return AsyncUSManager(
            us_auth_context=self.get_master_auth_context(),
            us_base_url=self.us_base_url,
            bi_context=rci,
            crypto_keys_config=self.crypto_keys_config,
            services_registry=services_registry,
        )

    def get_public_async_usm(
        self,
        rci: RequestContextInfo,
        services_registry: ServicesRegistry,
    ) -> AsyncUSManager:
        assert self.us_public_token is not None, "US public token must be set in factory to create USAuthContextMaster"

        return AsyncUSManager(
            us_auth_context=USAuthContextPublic(
                us_public_token=self.us_public_token,
            ),
            us_base_url=self.us_base_url,
            bi_context=rci,
            crypto_keys_config=self.crypto_keys_config,
            services_registry=services_registry,
        )

    # Sync
    def get_regular_sync_usm(
        self,
        rci: RequestContextInfo,
        services_registry: ServicesRegistry,
    ) -> SyncUSManager:
        return SyncUSManager(
            us_auth_context=self.get_regular_us_auth_ctx_from_rci(rci),
            us_base_url=self.us_base_url,
            bi_context=rci,
            crypto_keys_config=self.crypto_keys_config,
            services_registry=services_registry,
        )

    def get_master_sync_usm(
        self,
        rci: RequestContextInfo,
        services_registry: ServicesRegistry,
    ) -> SyncUSManager:
        return SyncUSManager(
            us_auth_context=self.get_master_auth_context(),
            us_base_url=self.us_base_url,
            bi_context=rci,
            crypto_keys_config=self.crypto_keys_config,
            services_registry=services_registry,
        )

    def get_embed_async_usm(
        self,
        rci: RequestContextInfo,
        services_registry: ServicesRegistry,
    ) -> AsyncUSManager:
        return AsyncUSManager(
            us_auth_context=self.def_embed_us_auth_ctx_from_rci(rci),
            us_base_url=self.us_base_url,
            bi_context=rci,
            crypto_keys_config=self.crypto_keys_config,
            services_registry=services_registry,
        )
