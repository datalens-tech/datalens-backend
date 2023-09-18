from importlib import metadata
from typing import (
    Collection,
    Optional,
    Type,
)

import dl_formula as package
from dl_formula.connectors.base.connector import FormulaConnector
from dl_formula.connectors.registration import CONN_REG_FORMULA

_CONNECTOR_EP_GROUP = f"{package.__name__}.connectors"


def _get_all_ep_connectors() -> dict[str, Type[FormulaConnector]]:
    entrypoints = list(metadata.entry_points().select(group=_CONNECTOR_EP_GROUP))  # type: ignore
    return {ep.name: ep.load() for ep in entrypoints}


def load_all_connectors() -> None:
    _get_all_ep_connectors()


def register_all_connectors(connector_ep_names: Optional[Collection[str]] = None) -> None:
    connectors = _get_all_ep_connectors()
    for ep_name, connector_cls in sorted(connectors.items()):
        if connector_ep_names is not None and ep_name not in connector_ep_names:
            continue
        CONN_REG_FORMULA.register_connector(connector_cls)
