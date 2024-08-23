from __future__ import annotations

import logging
from typing import (
    Collection,
    Optional,
)

import attr

from dl_configs.crypto_keys import (
    CryptoKeysConfig,
    get_single_key_crypto_keys_config,
)
from dl_configs.enums import RedisMode
from dl_configs.settings_submodels import RedisSettings
from dl_core.loader import CoreLibraryConfig


LOGGER = logging.getLogger(__name__)


@attr.s(frozen=True)
class UnitedStorageConfiguration:
    us_host: str = attr.ib(kw_only=True)
    us_master_token: str = attr.ib(kw_only=True)
    us_pg_dsn: str = attr.ib(kw_only=True)
    force: bool = attr.ib(kw_only=True, default=True)
    migrations: Optional[list[list[str]]] = attr.ib(kw_only=True, default=None)


@attr.s(frozen=True)
class RedisSettingMaker:
    redis_host: str = attr.ib(default="")
    redis_port: int = attr.ib(default=6379)
    redis_password: str = attr.ib(default="")
    redis_db_default: int = attr.ib(default=0)
    redis_db_cache: int = attr.ib(default=1)
    redis_db_mutation: int = attr.ib(default=2)
    redis_db_arq: int = attr.ib(default=11)

    def get_redis_settings(self, db: int) -> RedisSettings:
        return RedisSettings(  # type: ignore  # TODO: fix compatibility of models using `s_attrib` with mypy
            MODE=RedisMode.single_host,
            CLUSTER_NAME="",
            HOSTS=(self.redis_host,),
            PORT=self.redis_port,
            DB=db,
            PASSWORD=self.redis_password,
        )

    def get_redis_settings_default(self) -> RedisSettings:
        return self.get_redis_settings(self.redis_db_default)

    def get_redis_settings_cache(self) -> RedisSettings:
        return self.get_redis_settings(self.redis_db_cache)

    def get_redis_settings_mutation(self) -> RedisSettings:
        return self.get_redis_settings(self.redis_db_mutation)

    def get_redis_settings_arq(self) -> RedisSettings:
        return self.get_redis_settings(self.redis_db_arq)


# These are used only for creation of local environments in tests, not actual external ones
DEFAULT_FERNET_KEY = "h1ZpilcYLYRdWp7Nk8X1M1kBPiUi8rdjz9oBfHyUKIk="


@attr.s(frozen=True)
class CoreTestEnvironmentConfiguration:
    host_us_http: str = attr.ib(kw_only=True)
    port_us_http: int = attr.ib(kw_only=True)
    host_us_pg: str = attr.ib(kw_only=True)
    port_us_pg_5432: int = attr.ib(kw_only=True)
    us_master_token: str = attr.ib(kw_only=True)
    fernet_key: str = attr.ib(kw_only=True, default=DEFAULT_FERNET_KEY)

    core_connector_ep_names: Optional[Collection[str]] = attr.ib(kw_only=True, default=None)

    redis_host: str = attr.ib(default="")
    redis_port: int = attr.ib(default=6379)
    redis_password: str = attr.ib(default="")
    redis_db_default: int = attr.ib(default=0)
    redis_db_cache: int = attr.ib(default=1)
    redis_db_mutation: int = attr.ib(default=2)
    redis_db_arq: int = attr.ib(default=11)

    compeng_url: str = attr.ib(default="")

    def get_us_config(self) -> UnitedStorageConfiguration:
        return UnitedStorageConfiguration(
            us_master_token=self.us_master_token,
            us_host=f"http://{self.host_us_http}:{self.port_us_http}",
            us_pg_dsn=f"host={self.host_us_pg} port={self.port_us_pg_5432} user=us password=us dbname=us-db-ci_purgeable",
        )

    def get_crypto_keys_config(self) -> CryptoKeysConfig:
        return get_single_key_crypto_keys_config(key_id="0", key_value=self.fernet_key)

    def get_redis_setting_maker(self) -> RedisSettingMaker:
        return RedisSettingMaker(
            redis_host=self.redis_host,
            redis_port=self.redis_port,
            redis_password=self.redis_password,
            redis_db_default=self.redis_db_default,
            redis_db_cache=self.redis_db_cache,
            redis_db_mutation=self.redis_db_mutation,
            redis_db_arq=self.redis_db_arq,
        )

    def get_core_library_config(self) -> CoreLibraryConfig:
        return CoreLibraryConfig(
            core_connector_ep_names=self.core_connector_ep_names,
        )

    def get_compeng_url(self) -> str:
        return self.compeng_url
