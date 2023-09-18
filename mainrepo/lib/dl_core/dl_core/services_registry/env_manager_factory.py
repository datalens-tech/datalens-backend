from __future__ import annotations

from typing import TYPE_CHECKING

import attr

from dl_api_commons.base_models import RequestContextInfo
from dl_core.connections_security.base import (
    ConnectionSecurityManager,
    InsecureConnectionSecurityManager,
)
from dl_core.mdb_utils import (
    MDBDomainManager,
    MDBDomainManagerFactory,
    MDBDomainManagerSettings,
)
from dl_core.services_registry.env_manager_factory_base import EnvManagerFactory

if TYPE_CHECKING:
    from dl_core.connection_models import ConnectOptions


@attr.s
class DefaultEnvManagerFactory(EnvManagerFactory):
    def make_security_manager(self, request_context_info: RequestContextInfo) -> ConnectionSecurityManager:
        raise NotImplementedError()

    def make_mdb_domain_manager(self, settings: MDBDomainManagerSettings) -> MDBDomainManager:
        return MDBDomainManagerFactory(settings=settings).get_manager()

    @classmethod
    def mutate_conn_opts(cls, conn_opts: ConnectOptions) -> ConnectOptions:
        return conn_opts


class InsecureEnvManagerFactory(DefaultEnvManagerFactory):
    def make_security_manager(self, request_context_info: RequestContextInfo) -> ConnectionSecurityManager:
        return InsecureConnectionSecurityManager()
