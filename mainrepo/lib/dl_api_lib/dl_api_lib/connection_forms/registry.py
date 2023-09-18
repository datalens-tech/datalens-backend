from __future__ import annotations

from typing import Type

from dl_api_connector.form_config.models.base import ConnectionFormFactory
from dl_constants.enums import ConnectionType


class NoForm(Exception):
    pass


CONN_FORM_FACTORY_BY_TYPE: dict[ConnectionType, Type[ConnectionFormFactory]] = {}


def register_connection_form_factory_cls(conn_type: ConnectionType, factory_cls: Type[ConnectionFormFactory]) -> None:
    CONN_FORM_FACTORY_BY_TYPE[conn_type] = factory_cls


def get_connection_form_factory_cls(conn_type: ConnectionType) -> Type[ConnectionFormFactory]:
    if (conn_form := CONN_FORM_FACTORY_BY_TYPE.get(conn_type)) is not None:
        return conn_form
    raise NoForm(conn_type)
