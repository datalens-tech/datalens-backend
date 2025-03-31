from __future__ import annotations


from dl_constants.enums import ConnectionType
from dl_core.connectors.base.lifecycle import (
    ConnectionLifecycleManager,
    DefaultConnectionLifecycleManager,
)
from dl_core.connectors.base.schema_migration import (
    ConnectionSchemaMigration,
    DefaultConnectionSchemaMigration,
)
from dl_core.us_connection_base import (
    ConnectionBase,
    UnknownConnection,
)


CONNECTION_TYPES: dict[ConnectionType, type[ConnectionBase]] = {
    ConnectionType.unknown: UnknownConnection,
}
CONNECTION_LIFECYCLE_MGR_CLASSES: dict[ConnectionType, type[ConnectionLifecycleManager]] = {
    ConnectionType.unknown: DefaultConnectionLifecycleManager,
}
CONNECTION_SHEMA_MIGRATION_CLASSES: dict[str, type[ConnectionSchemaMigration]] = {
    ConnectionType.unknown.name: DefaultConnectionSchemaMigration,
}


def get_connection_class(conn_type: ConnectionType) -> type[ConnectionBase]:
    """Return class for given connection type"""
    return CONNECTION_TYPES[conn_type]


def get_lifecycle_manager_cls(conn_type: ConnectionType) -> type[ConnectionLifecycleManager]:
    """Return class for given connection type"""
    return CONNECTION_LIFECYCLE_MGR_CLASSES[conn_type]


def get_schema_migration_cls(conn_type_name: str) -> type[ConnectionSchemaMigration]:
    """Return class for given connection type"""
    return CONNECTION_SHEMA_MIGRATION_CLASSES.get(conn_type_name, DefaultConnectionSchemaMigration)


def register_connection_class(
    new_conn_cls: type[ConnectionBase],
    conn_type: ConnectionType,
    lifecycle_manager_cls: type[ConnectionLifecycleManager] = DefaultConnectionLifecycleManager,
    schema_migration_cls: type[ConnectionSchemaMigration] = DefaultConnectionSchemaMigration,
    allow_ct_override: bool = False,
) -> None:
    if conn_type is None:
        raise ValueError(f"Connection type for {new_conn_cls} is None")

    if not allow_ct_override:
        if conn_type in CONNECTION_TYPES:
            raise ValueError(f"Attempt to duplicated connection type registration: {conn_type}")

        if new_conn_cls in CONNECTION_TYPES.values():
            raise ValueError(f"Attempt to duplicated connection class registration: {new_conn_cls}")

    CONNECTION_TYPES[conn_type] = new_conn_cls
    CONNECTION_LIFECYCLE_MGR_CLASSES[conn_type] = lifecycle_manager_cls
    CONNECTION_SHEMA_MIGRATION_CLASSES[conn_type.name] = schema_migration_cls
