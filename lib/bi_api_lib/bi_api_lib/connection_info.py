from typing import Type

from bi_constants.enums import ConnectionType

from bi_api_connector.connection_info import ConnectionInfoProvider

from bi_api_lib.connectors.chydb.connection_info import CHYDBConnectionInfoProvider


class InfoProviderNotFound(Exception):
    pass


CONNECTOR_INFO_PROVIDER_CLS_BY_TYPE: dict[ConnectionType, Type[ConnectionInfoProvider]] = {}


def register_connector_info_provider_cls(
        conn_type: ConnectionType,
        info_provider_cls: Type[ConnectionInfoProvider],
) -> None:
    CONNECTOR_INFO_PROVIDER_CLS_BY_TYPE[conn_type] = info_provider_cls


def get_connector_info_provider_cls(conn_type: ConnectionType) -> Type[ConnectionInfoProvider]:
    try:
        return CONNECTOR_INFO_PROVIDER_CLS_BY_TYPE[conn_type]
    except KeyError as exc:
        raise InfoProviderNotFound(conn_type) from exc


register_connector_info_provider_cls(ConnectionType.chydb, CHYDBConnectionInfoProvider)
