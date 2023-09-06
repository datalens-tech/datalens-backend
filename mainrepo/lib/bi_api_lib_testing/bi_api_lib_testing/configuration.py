from __future__ import annotations

from typing import Any

import attr

from bi_api_lib.connector_availability.base import ConnectorAvailabilityConfig
from bi_configs.environments import InternalTestingInstallation, TestsInstallation

from bi_core_testing.configuration import CoreTestEnvironmentConfigurationBase


@attr.s(kw_only=True)
class BiApiTestEnvironmentConfiguration:
    core_test_config: CoreTestEnvironmentConfigurationBase = attr.ib()

    dls_host: str = attr.ib(default=InternalTestingInstallation.DATALENS_API_LB_DLS_BASE_URL)
    dls_key: str = attr.ib(default='_tests_dls_api_key_')

    ext_query_executer_secret_key: str = attr.ib()

    mutation_cache_enabled: bool = attr.ib(default=True)

    bi_compeng_pg_url: str = attr.ib(default='')

    redis_host: str = attr.ib(default='')
    redis_port: int = attr.ib(default=6379)
    redis_password: str = attr.ib(default='')
    redis_db_default: int = attr.ib(default=0)
    redis_db_cache: int = attr.ib(default=1)
    redis_db_mutation: int = attr.ib(default=2)
    redis_db_arq: int = attr.ib(default=11)

    connector_availability: ConnectorAvailabilityConfig = attr.ib(default=TestsInstallation.CONNECTOR_AVAILABILITY)
    connector_whitelist: str = attr.ib(default=TestsInstallation.CONNECTOR_WHITELIST)

    def clone(self, **kwargs: Any) -> BiApiTestEnvironmentConfiguration:
        return attr.evolve(self, **kwargs)
