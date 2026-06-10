from dl_core.lifecycle.base import EntryLifecycleManager
from dl_core.us_connection_base import ConnectionBase


class ConnectionLifecycleManager[CONNECTION_TV: ConnectionBase](EntryLifecycleManager[CONNECTION_TV]):
    pass


class DefaultConnectionLifecycleManager(ConnectionLifecycleManager[ConnectionBase]):
    ENTRY_CLS = ConnectionBase
