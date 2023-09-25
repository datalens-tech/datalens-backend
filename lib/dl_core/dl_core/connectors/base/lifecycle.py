from typing import (
    Generic,
    TypeVar,
)

from dl_core.lifecycle.base import EntryLifecycleManager
from dl_core.us_connection_base import ConnectionBase


_CONNECTION_TV = TypeVar("_CONNECTION_TV", bound=ConnectionBase)


class ConnectionLifecycleManager(EntryLifecycleManager[_CONNECTION_TV], Generic[_CONNECTION_TV]):
    pass


class DefaultConnectionLifecycleManager(ConnectionLifecycleManager[ConnectionBase]):
    ENTRY_CLS = ConnectionBase
