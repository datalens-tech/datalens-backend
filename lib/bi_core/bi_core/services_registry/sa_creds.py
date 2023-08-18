import enum
from abc import ABC, abstractmethod
from typing import Optional

import attr

from bi_cloud_integration.local_metadata import get_yc_service_token_local
from bi_cloud_integration.yc_ts_client import get_yc_service_token


class SACredsMode(enum.Enum):
    local_metadata = enum.auto()
    env_key_data = enum.auto()


@attr.s
class SACredsSettings:
    mode: SACredsMode = attr.ib()
    env_key_data: Optional[dict[str, str]] = attr.ib(default=None, repr=False)


@attr.s
class SACredsRetrieverBase(ABC):
    """
    Base class for YC service account credentials (IAM token) retrieving
    """
    sa_creds_settings: SACredsSettings = attr.ib()

    @abstractmethod
    async def get_sa_token(self) -> str:
        raise NotImplementedError


class SACredsRetrieverLocalMetadata(SACredsRetrieverBase):
    """
    Retrieves token from local metadata of YC virtual machine (for VM service account)
    """
    async def get_sa_token(self) -> str:
        return await get_yc_service_token_local()


@attr.s
class SACredsRetrieverEnvKeyData(SACredsRetrieverBase):
    """
    Retrieves token for service account which key data is specified in env
    """
    ts_endpoint: Optional[str] = attr.ib(default=None)

    async def get_sa_token(self) -> str:
        assert self.sa_creds_settings.env_key_data is not None
        assert self.ts_endpoint is not None
        return await get_yc_service_token(
            key_data=self.sa_creds_settings.env_key_data,
            yc_ts_endpoint=self.ts_endpoint,
        )


@attr.s
class SACredsRetrieverFactory:
    sa_creds_settings: SACredsSettings = attr.ib()
    ts_endpoint: Optional[str] = attr.ib(default=None)

    def get_retriever(self) -> SACredsRetrieverBase:
        if self.sa_creds_settings.mode == SACredsMode.local_metadata:
            return SACredsRetrieverLocalMetadata(sa_creds_settings=self.sa_creds_settings)
        elif self.sa_creds_settings.mode == SACredsMode.env_key_data:
            return SACredsRetrieverEnvKeyData(
                sa_creds_settings=self.sa_creds_settings,
                ts_endpoint=self.ts_endpoint
            )
        else:
            raise ValueError(f'Unsupported sa_creds mode: {self.sa_creds_settings.mode}')
