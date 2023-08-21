from __future__ import annotations

import abc
import logging

import attr

from bi_configs.crypto_keys import CryptoKeysConfig, get_single_key_crypto_keys_config
from bi_core.loader import CoreLibraryConfig

LOGGER = logging.getLogger(__name__)


@attr.s(frozen=True)
class UnitedStorageConfiguration:
    us_host: str = attr.ib(kw_only=True)
    us_master_token: str = attr.ib(kw_only=True)
    us_pg_dsn: str = attr.ib(kw_only=True)
    force: bool = attr.ib(kw_only=True, default=True)


@attr.s(frozen=True)
class CoreTestEnvironmentConfigurationBase(abc.ABC):
    @abc.abstractmethod
    def get_us_config(self) -> UnitedStorageConfiguration:
        raise NotImplementedError

    @abc.abstractmethod
    def get_crypto_keys_config(self) -> CryptoKeysConfig:
        raise NotImplementedError

    @abc.abstractmethod
    def get_core_library_config(self) -> CoreLibraryConfig:
        raise NotImplementedError


# These are used only for creation of local environments in tests, not actual external ones
DEFAULT_FERNET_KEY = 'h1ZpilcYLYRdWp7Nk8X1M1kBPiUi8rdjz9oBfHyUKIk='


@attr.s(frozen=True)
class DefaultCoreTestConfiguration(CoreTestEnvironmentConfigurationBase):
    host_us_http: str = attr.ib(kw_only=True)
    port_us_http: int = attr.ib(kw_only=True)
    host_us_pg: str = attr.ib(kw_only=True)
    port_us_pg_5432: int = attr.ib(kw_only=True)
    us_master_token: str = attr.ib(kw_only=True)
    fernet_key: str = attr.ib(kw_only=True, default=DEFAULT_FERNET_KEY)
    core_library_config: CoreLibraryConfig = attr.ib(kw_only=True, default=CoreLibraryConfig())

    def get_us_config(self) -> UnitedStorageConfiguration:
        return UnitedStorageConfiguration(
            us_master_token=self.us_master_token,
            us_host=f'http://{self.host_us_http}:{self.port_us_http}',
            us_pg_dsn=f'host={self.host_us_pg} port={self.port_us_pg_5432} user=us password=us dbname=us-db-ci_purgeable'
        )

    def get_crypto_keys_config(self) -> CryptoKeysConfig:
        return get_single_key_crypto_keys_config(key_id="0", key_value=self.fernet_key)

    def get_core_library_config(self) -> CoreLibraryConfig:
        return self.core_library_config
