from __future__ import annotations

from typing import Any

import attr

from bi_configs.environments import InternalTestingInstallation, TestsInstallation

from bi_core_testing.configuration import CoreTestEnvironmentConfigurationBase


@attr.s
class BiApiTestEnvironmentConfiguration:
    core_test_config: CoreTestEnvironmentConfigurationBase = attr.ib(kw_only=True)

    dls_host: str = attr.ib(kw_only=True, default=InternalTestingInstallation.DATALENS_API_LB_DLS_BASE_URL)
    dls_key: str = attr.ib(kw_only=True, default='_tests_dls_api_key_')

    ext_query_executer_secret_key: str = attr.ib(kw_only=True)

    mutation_cache_enabled: bool = attr.ib(kw_only=True, default=True)

    bi_compeng_pg_url: str = attr.ib(kw_only=True, default='')

    redis_host: str = attr.ib(kw_only=True, default='')
    redis_port: int = attr.ib(kw_only=True, default=6379)
    redis_password: str = attr.ib(kw_only=True, default='')
    redis_db_default: int = attr.ib(kw_only=True, default=0)
    redis_db_cache: int = attr.ib(kw_only=True, default=1)
    redis_db_mutation: int = attr.ib(kw_only=True, default=2)
    redis_db_arq: int = attr.ib(kw_only=True, default=11)

    connector_whitelist: str = attr.ib(kw_only=True, default=TestsInstallation.CONNECTOR_WHITELIST)

    def clone(self, **kwargs: Any) -> BiApiTestEnvironmentConfiguration:
        return attr.evolve(self, **kwargs)
