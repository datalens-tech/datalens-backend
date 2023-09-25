from typing import (
    Collection,
    Optional,
    Type,
)

from dl_core.connectors.base.connector import CoreConnector
from dl_core.connectors.base.registrator import CONN_REG_CORE
from dl_core.connectors.registry import get_all_connectors


def _register_connector(connector_cls: Type[CoreConnector]) -> None:
    CONN_REG_CORE.register_connector(connector_cls)


def load_all_connectors() -> None:
    get_all_connectors()


def register_all_connectors(connector_ep_names: Optional[Collection[str]] = None) -> None:
    for ep_name, connector_cls in sorted(get_all_connectors().items()):
        if connector_ep_names is not None and ep_name not in connector_ep_names:
            continue
        _register_connector(connector_cls)
