from __future__ import annotations

from typing import Type

from bi_api_connector.form_config.models.base import ConnectionFormFactory
from bi_constants.enums import ConnectionType

from bi_api_lib.connectors.chydb.connection_form.form_config import CHYDBConnectionFormFactory


class NoForm(Exception):
    pass


CONN_FORM_FACTORY_BY_TYPE: dict[ConnectionType, Type[ConnectionFormFactory]] = {}


def register_connection_form_factory_cls(conn_type: ConnectionType, factory_cls: Type[ConnectionFormFactory]) -> None:
    CONN_FORM_FACTORY_BY_TYPE[conn_type] = factory_cls


def get_connection_form_factory_cls(conn_type: ConnectionType) -> Type[ConnectionFormFactory]:
    try:
        return CONN_FORM_FACTORY_BY_TYPE[conn_type]
    except KeyError as exc:
        raise NoForm(conn_type) from exc


register_connection_form_factory_cls(ConnectionType.chydb, CHYDBConnectionFormFactory)
