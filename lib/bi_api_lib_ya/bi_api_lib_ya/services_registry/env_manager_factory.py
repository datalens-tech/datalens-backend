from __future__ import annotations

from typing import List

import attr

from bi_api_lib_ya.connections_security.base import (
    CloudConnectionSecurityManager,
    InternalConnectionSecurityManager,
    MDBConnectionSafetyChecker,
    SamplesConnectionSafetyChecker,
)
from dl_api_commons.base_models import RequestContextInfo
from dl_core.connections_security.base import (
    ConnectionSecurityManager,
    InsecureConnectionSafetyChecker,
    NonUserInputConnectionSafetyChecker,
)
from dl_core.services_registry.env_manager_factory import DefaultEnvManagerFactory


class IntranetEnvManagerFactory(DefaultEnvManagerFactory):
    def make_security_manager(self, request_context_info: RequestContextInfo) -> ConnectionSecurityManager:
        return InternalConnectionSecurityManager(
            conn_sec_checkers=[
                InsecureConnectionSafetyChecker(),
            ],
        )


@attr.s
class CloudEnvManagerFactory(DefaultEnvManagerFactory):
    samples_ch_hosts: List[str] = attr.ib(kw_only=True)

    def make_security_manager(self, request_context_info: RequestContextInfo) -> ConnectionSecurityManager:
        samples_ch_hosts = frozenset(self.samples_ch_hosts)
        return CloudConnectionSecurityManager(
            conn_sec_checkers=[
                NonUserInputConnectionSafetyChecker(),
                SamplesConnectionSafetyChecker(samples_hosts=samples_ch_hosts),
                MDBConnectionSafetyChecker(),
            ],
            samples_ch_hosts=samples_ch_hosts,
        )


@attr.s
class DataCloudEnvManagerFactory(DefaultEnvManagerFactory):
    samples_ch_hosts: List[str] = attr.ib(kw_only=True)

    def make_security_manager(self, request_context_info: RequestContextInfo) -> ConnectionSecurityManager:
        samples_ch_hosts = frozenset(self.samples_ch_hosts)
        return CloudConnectionSecurityManager(
            conn_sec_checkers=[
                NonUserInputConnectionSafetyChecker(),
                SamplesConnectionSafetyChecker(samples_hosts=samples_ch_hosts),
                MDBConnectionSafetyChecker(),
            ],
            samples_ch_hosts=samples_ch_hosts,
        )
