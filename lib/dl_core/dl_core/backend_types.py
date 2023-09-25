from dl_constants.enums import (
    ConnectionType,
    SourceBackendType,
)


_CONNECTION_BACKEND_TYPES: dict[ConnectionType, SourceBackendType] = {}


def register_connection_backend_type(conn_type: ConnectionType, backend_type: SourceBackendType) -> None:
    if (registered_backend_type := _CONNECTION_BACKEND_TYPES.get(conn_type)) is not None:
        assert registered_backend_type == backend_type
    else:
        _CONNECTION_BACKEND_TYPES[conn_type] = backend_type


def get_backend_type(conn_type: ConnectionType) -> SourceBackendType:
    """Return class for given connection type"""
    return _CONNECTION_BACKEND_TYPES[conn_type]
