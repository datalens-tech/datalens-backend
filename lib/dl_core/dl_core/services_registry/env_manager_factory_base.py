from __future__ import annotations

import abc
from typing import TYPE_CHECKING

from dl_core.connections_security.base import ConnectionSecurityManager


if TYPE_CHECKING:
    from dl_core.connection_models import ConnectOptions


class EnvManagerFactory(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def make_security_manager(self) -> ConnectionSecurityManager:
        raise NotImplementedError

    @classmethod
    @abc.abstractmethod
    def mutate_conn_opts(cls, conn_opts: ConnectOptions) -> ConnectOptions:
        """
        Location for adding env-specific but non-app-specific alterations to
        the connect options.
        """
        raise NotImplementedError
