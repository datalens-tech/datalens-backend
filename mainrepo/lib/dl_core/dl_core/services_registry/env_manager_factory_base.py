from __future__ import annotations

import abc
from typing import TYPE_CHECKING

from dl_api_commons.base_models import RequestContextInfo
from dl_core.connections_security.base import ConnectionSecurityManager

if TYPE_CHECKING:
    from dl_core.connection_models import ConnectOptions
    from dl_core.mdb_utils import (
        MDBDomainManager,
        MDBDomainManagerSettings,
    )


class EnvManagerFactory(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def make_security_manager(self, request_context_info: RequestContextInfo) -> ConnectionSecurityManager:
        raise NotImplementedError

    @abc.abstractmethod
    def make_mdb_domain_manager(self, settings: MDBDomainManagerSettings) -> MDBDomainManager:
        raise NotImplementedError

    @classmethod
    @abc.abstractmethod
    def mutate_conn_opts(cls, conn_opts: ConnectOptions) -> ConnectOptions:
        """
        Location for adding env-specific but non-app-specific alterations to
        the connect options.
        """
        raise NotImplementedError
