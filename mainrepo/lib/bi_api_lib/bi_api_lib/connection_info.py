from typing import Type

from bi_constants.enums import ConnectionType

from bi_api_connector.connection_info import ConnectionInfoProvider


class InfoProviderNotFound(Exception):
    pass


CONNECTOR_INFO_PROVIDER_CLS_BY_TYPE: dict[ConnectionType, Type[ConnectionInfoProvider]] = {}


def register_connector_info_provider_cls(
        conn_type: ConnectionType,
        info_provider_cls: Type[ConnectionInfoProvider],
) -> None:
    CONNECTOR_INFO_PROVIDER_CLS_BY_TYPE[conn_type] = info_provider_cls


def get_connector_info_provider_cls(conn_type: ConnectionType) -> Type[ConnectionInfoProvider]:
    if (conn_info_provider := CONNECTOR_INFO_PROVIDER_CLS_BY_TYPE.get(conn_type)) is not None:
        return conn_info_provider
    raise InfoProviderNotFound(conn_type)
