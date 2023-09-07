from __future__ import annotations

import attr

from bi_configs.environments import InternalTestingInstallation

from bi_api_lib_testing.configuration import BiApiTestEnvironmentConfiguration


@attr.s(kw_only=True)
class BiApiTestEnvironmentConfigurationPrivate(BiApiTestEnvironmentConfiguration):
    dls_host: str = attr.ib(default=InternalTestingInstallation.DATALENS_API_LB_DLS_BASE_URL)
    dls_key: str = attr.ib(default='_tests_dls_api_key_')
