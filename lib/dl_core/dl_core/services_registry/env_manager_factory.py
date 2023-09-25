from __future__ import annotations

from typing import TYPE_CHECKING

import attr

from dl_core.connections_security.base import (
    ConnectionSecurityManager,
    InsecureConnectionSecurityManager,
)
from dl_core.services_registry.env_manager_factory_base import EnvManagerFactory


if TYPE_CHECKING:
    from dl_core.connection_models import ConnectOptions


@attr.s
class DefaultEnvManagerFactory(EnvManagerFactory):
    def make_security_manager(self) -> ConnectionSecurityManager:
        raise NotImplementedError()

    @classmethod
    def mutate_conn_opts(cls, conn_opts: ConnectOptions) -> ConnectOptions:
        return conn_opts


class InsecureEnvManagerFactory(DefaultEnvManagerFactory):
    def make_security_manager(self) -> ConnectionSecurityManager:
        return InsecureConnectionSecurityManager()
