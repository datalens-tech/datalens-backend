from __future__ import annotations

from typing import (
    Collection,
    Optional,
)

import attr

from dl_api_lib.loader import ApiLibraryConfig
from dl_configs.connector_availability import ConnectorAvailabilityConfigSettings
from dl_core_testing.configuration import CoreTestEnvironmentConfigurationBase


@attr.s(kw_only=True)
class ApiTestEnvironmentConfiguration:
    core_test_config: CoreTestEnvironmentConfigurationBase = attr.ib()

    ext_query_executer_secret_key: str = attr.ib()

    mutation_cache_enabled: bool = attr.ib(default=True)

    bi_compeng_pg_url: str = attr.ib(default="")

    file_uploader_api_host: str = attr.ib(default="http://127.0.0.1")
    file_uploader_api_port: int = attr.ib(default=9999)

    redis_host: str = attr.ib(default="")
    redis_port: int = attr.ib(default=6379)
    redis_password: str = attr.ib(default="")
    redis_db_default: int = attr.ib(default=0)
    redis_db_cache: int = attr.ib(default=1)
    redis_db_mutation: int = attr.ib(default=2)
    redis_db_arq: int = attr.ib(default=11)

    connector_availability_settings: ConnectorAvailabilityConfigSettings = attr.ib(
        factory=ConnectorAvailabilityConfigSettings,
    )
    api_connector_ep_names: Optional[Collection[str]] = attr.ib(default=None)

    def get_api_library_config(self) -> ApiLibraryConfig:
        return ApiLibraryConfig(
            api_connector_ep_names=self.api_connector_ep_names,
            core_lib_config=self.core_test_config.get_core_library_config(),
        )
