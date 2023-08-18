from typing import Type

from bi_core.connectors.registry import get_all_connectors
from bi_core.connectors.base.registrator import CONN_REG_CORE
from bi_core.connectors.base.connector import CoreConnector


def _register_connector(connector_cls: Type[CoreConnector]) -> None:
    CONN_REG_CORE.register_connector(connector_cls)


def register_all_connectors() -> None:
    for _, connector_cls in sorted(get_all_connectors().items()):
        _register_connector(connector_cls)
