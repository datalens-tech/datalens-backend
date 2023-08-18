from typing import Generic, TypeVar

from bi_core.lifecycle.base import EntryLifecycleManager
from bi_core.us_connection_base import ConnectionBase


_CONNECTION_TV = TypeVar('_CONNECTION_TV', bound=ConnectionBase)


class ConnectionLifecycleManager(EntryLifecycleManager[_CONNECTION_TV], Generic[_CONNECTION_TV]):

    def pre_save_hook(self) -> None:
        # TODO: Move to some subclass. This was moved from the `ConnectionBase` class as-is
        if hasattr(self.entry.data, 'mdb_cluster_id'):
            self.entry.meta['mdb_cluster_id'] = self.entry.data.mdb_cluster_id
        if hasattr(self.entry.data, 'mdb_folder_id'):
            self.entry.meta['mdb_folder_id'] = self.entry.data.mdb_folder_id


class DefaultConnectionLifecycleManager(ConnectionLifecycleManager[ConnectionBase]):
    ENTRY_CLS = ConnectionBase
