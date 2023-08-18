from typing import Optional

from bi_constants.enums import ConnectionType
from bi_connector_metrica.core.constants import CONNECTION_TYPE_METRICA_API, CONNECTION_TYPE_APPMETRICA_API


CONNECTOR_ALIAS_BY_TYPE: dict[ConnectionType, Optional[str]] = {}


def register_connector_alias(conn_type: ConnectionType, alias: Optional[str]) -> None:
    CONNECTOR_ALIAS_BY_TYPE[conn_type] = alias


def get_connector_alias(conn_type: ConnectionType) -> Optional[str]:
    return CONNECTOR_ALIAS_BY_TYPE.get(conn_type, None)


register_connector_alias(CONNECTION_TYPE_METRICA_API, 'metrica')
register_connector_alias(CONNECTION_TYPE_APPMETRICA_API, 'appmetrica')
