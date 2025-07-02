from typing import Optional

import attr
import flask

from dl_api_commons.base_models import RequestContextInfo
from dl_configs.crypto_keys import CryptoKeysConfig
from dl_constants.api_constants import DLHeadersCommon
from dl_core.enums import USApiType
from dl_core.exc import InvalidRequestError
from dl_core.retrier.policy import BaseRetryPolicyFactory
from dl_core.services_registry import ServicesRegistry
from dl_core.united_storage_client import (
    USAuthContextEmbed,
    USAuthContextMaster,
    USAuthContextPublic,
    USAuthContextRegular,
)
from dl_core.us_manager.us_manager_async import AsyncUSManager
from dl_core.us_manager.us_manager_sync import SyncUSManager


@attr.s(frozen=True)
class USMFactory:
    us_base_url: str = attr.ib()
    crypto_keys_config: Optional[CryptoKeysConfig] = attr.ib()
    ca_data: bytes = attr.ib()
    retry_policy_factory: BaseRetryPolicyFactory = attr.ib()
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

    def get_master_auth_context_from_headers(self) -> USAuthContextMaster:
        us_master_token = flask.request.headers.get(DLHeadersCommon.US_MASTER_TOKEN.value)
        if us_master_token is None:
            raise InvalidRequestError(
                f"US master token must be set in header {DLHeadersCommon.US_MASTER_TOKEN.value} to create USAuthContextMaster"
            )
        return USAuthContextMaster(us_master_token=us_master_token)

    def get_ca_data(self) -> bytes:
        assert self.ca_data is not None, "ca_data must be set in factory to create AsyncUSClient"
        return self.ca_data

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
            ca_data=self.get_ca_data(),
            retry_policy_factory=self.retry_policy_factory,
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
            ca_data=self.get_ca_data(),
            retry_policy_factory=self.retry_policy_factory,
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
            ca_data=self.get_ca_data(),
            retry_policy_factory=self.retry_policy_factory,
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
            retry_policy_factory=self.retry_policy_factory,
        )

    def get_master_sync_usm(
        self, rci: RequestContextInfo, services_registry: ServicesRegistry, is_token_stored: Optional[bool] = True
    ) -> SyncUSManager:
        return SyncUSManager(
            us_auth_context=self.get_master_auth_context()
            if is_token_stored
            else self.get_master_auth_context_from_headers(),
            us_base_url=self.us_base_url,
            bi_context=rci,
            crypto_keys_config=self.crypto_keys_config,
            services_registry=services_registry,
            retry_policy_factory=self.retry_policy_factory,
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
            ca_data=self.get_ca_data(),
            retry_policy_factory=self.retry_policy_factory,
        )

    def get_async_usm(
        self,
        rci: RequestContextInfo,
        services_registry: ServicesRegistry,
        us_api_type: USApiType,
    ) -> AsyncUSManager:
        usm_getters = {
            USApiType.v1: self.get_regular_async_usm,
            USApiType.public: self.get_public_async_usm,
            USApiType.private: self.get_master_async_usm,
            USApiType.embeds: self.get_embed_async_usm,
        }
        get_usm = usm_getters.get(us_api_type, self.get_regular_async_usm)
        return get_usm(rci=rci, services_registry=services_registry)
