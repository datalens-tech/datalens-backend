from dl_constants.enums import ConnectionType


CONNECTOR_ALIAS_BY_TYPE: dict[ConnectionType, str | None] = {}


def register_connector_alias(conn_type: ConnectionType, alias: str | None) -> None:
    CONNECTOR_ALIAS_BY_TYPE[conn_type] = alias


def get_connector_alias(conn_type: ConnectionType) -> str | None:
    return CONNECTOR_ALIAS_BY_TYPE.get(conn_type, None)
