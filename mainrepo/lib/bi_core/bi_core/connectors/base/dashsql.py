from bi_constants.enums import ConnectionType

_CONNECTION_DASHSQL_KEYS: dict[ConnectionType, frozenset[str]] = {}


def register_custom_dash_sql_key_names(conn_type: ConnectionType, key_names: frozenset[str]) -> None:
    if (registered_key_names := _CONNECTION_DASHSQL_KEYS.get(conn_type)) is not None:
        assert registered_key_names == key_names
    else:
        _CONNECTION_DASHSQL_KEYS[conn_type] = key_names


def get_custom_dash_sql_key_names(conn_type: ConnectionType) -> frozenset[str]:
    return _CONNECTION_DASHSQL_KEYS[conn_type]
