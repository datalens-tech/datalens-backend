from __future__ import annotations

from typing import TYPE_CHECKING, List

import attr

from bi_api_commons.base_models import RequestContextInfo
from bi_core.connections_security.base import (
    CloudConnectionSecurityManager,
    ConnectionSecurityManager,
    InternalConnectionSecurityManager,
)
from bi_core.services_registry.env_manager_factory_base import EnvManagerFactory
from bi_core.mdb_utils import MDBDomainManager, MDBDomainManagerSettings, MDBDomainManagerFactory

if TYPE_CHECKING:
    from bi_core.connection_models import ConnectOptions


@attr.s
class DefaultEnvManagerFactory(EnvManagerFactory):
    def make_security_manager(self, request_context_info: RequestContextInfo) -> ConnectionSecurityManager:
        raise NotImplementedError()

    def make_mdb_domain_manager(self, settings: MDBDomainManagerSettings) -> MDBDomainManager:
        return MDBDomainManagerFactory(settings=settings).get_manager()

    @classmethod
    def mutate_conn_opts(cls, conn_opts: ConnectOptions) -> ConnectOptions:
        return conn_opts


class IntranetEnvManagerFactory(DefaultEnvManagerFactory):
    def make_security_manager(self, request_context_info: RequestContextInfo) -> ConnectionSecurityManager:
        return InternalConnectionSecurityManager()


# noinspection PyDataclass
@attr.s
class CloudEnvManagerFactory(DefaultEnvManagerFactory):
    samples_ch_hosts: List[str] = attr.ib(kw_only=True)

    def make_security_manager(self, request_context_info: RequestContextInfo) -> ConnectionSecurityManager:
        return CloudConnectionSecurityManager(
            samples_ch_hosts=frozenset(self.samples_ch_hosts),
        )


# noinspection PyDataclass
@attr.s
class DataCloudEnvManagerFactory(DefaultEnvManagerFactory):
    samples_ch_hosts: List[str] = attr.ib(kw_only=True)

    def make_security_manager(self, request_context_info: RequestContextInfo) -> ConnectionSecurityManager:
        return CloudConnectionSecurityManager(
            samples_ch_hosts=frozenset(self.samples_ch_hosts),
        )
