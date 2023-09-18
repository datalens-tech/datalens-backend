from typing import Type

from marshmallow import Schema

from dl_core.us_connection_base import (
    ConnectionBase,
    UnknownConnection,
)
from dl_core.us_manager.storage_schemas.connection import ConnectionBaseDataStorageSchema

MAP_TYPE_TO_SCHEMA_MAP_TYPE_TO_SCHEMA = {}


def register_connection_schema(conn_cls: Type[ConnectionBase], schema_cls: Type[Schema]) -> None:
    MAP_TYPE_TO_SCHEMA_MAP_TYPE_TO_SCHEMA[conn_cls.DataModel] = schema_cls  # type: ignore


class UnknownConnectionStorageSchema(ConnectionBaseDataStorageSchema[UnknownConnection.DataModel]):
    TARGET_CLS = UnknownConnection.DataModel


register_connection_schema(UnknownConnection, UnknownConnectionStorageSchema)
