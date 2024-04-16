from typing import (
    Generic,
    TypeVar,
)

from dl_core.lifecycle.base import EntryLifecycleManager
from dl_core.us_connection_base import ConnectionBase


_CONNECTION_TV = TypeVar("_CONNECTION_TV", bound=ConnectionBase)


class ConnectionLifecycleManager(EntryLifecycleManager[_CONNECTION_TV], Generic[_CONNECTION_TV]):
    def _set_connector_settings_to_connection(self) -> None:
        connectors_settings = self._service_registry.get_connectors_settings(self.entry.conn_type)
        if connectors_settings is not None:
            # TODO: Log
            assert isinstance(connectors_settings, self.entry.settings_type)
            self.entry.set_connector_settings(connectors_settings)

    async def post_init_async_hook(self) -> None:
        await super().post_init_async_hook()
        self._set_connector_settings_to_connection()


class DefaultConnectionLifecycleManager(ConnectionLifecycleManager[ConnectionBase]):
    ENTRY_CLS = ConnectionBase
