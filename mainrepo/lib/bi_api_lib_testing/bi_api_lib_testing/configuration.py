from __future__ import annotations

from typing import Optional

import attr

from bi_configs.connector_availability import ConnectorAvailabilityConfigSettings
from bi_core_testing.configuration import CoreTestEnvironmentConfigurationBase


@attr.s(kw_only=True)
class BiApiTestEnvironmentConfiguration:
    core_test_config: CoreTestEnvironmentConfigurationBase = attr.ib()

    ext_query_executer_secret_key: str = attr.ib()

    mutation_cache_enabled: bool = attr.ib(default=True)

    bi_compeng_pg_url: str = attr.ib(default="")

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
    bi_api_connector_whitelist: Optional[list[str]] = attr.ib(factory=list)
    core_connector_whitelist: Optional[list[str]] = attr.ib(factory=list)
