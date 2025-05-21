from dl_constants.enums import ConnectionType


_EXPORT_IMPORT_ALLOWED_BY_CONN_TYPE: dict[ConnectionType, bool] = {}


def register_export_import_allowed(conn_type: ConnectionType, value: bool) -> None:
    _EXPORT_IMPORT_ALLOWED_BY_CONN_TYPE[conn_type] = value


def is_export_import_allowed(conn_type: ConnectionType) -> bool:
    return _EXPORT_IMPORT_ALLOWED_BY_CONN_TYPE[conn_type]
