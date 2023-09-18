from typing import Optional

from dl_constants.enums import ConnectionType

CONNECTOR_ALIAS_BY_TYPE: dict[ConnectionType, Optional[str]] = {}


def register_connector_alias(conn_type: ConnectionType, alias: Optional[str]) -> None:
    CONNECTOR_ALIAS_BY_TYPE[conn_type] = alias


def get_connector_alias(conn_type: ConnectionType) -> Optional[str]:
    return CONNECTOR_ALIAS_BY_TYPE.get(conn_type, None)
