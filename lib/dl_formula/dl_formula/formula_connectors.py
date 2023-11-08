from importlib import metadata
from typing import (
    Collection,
    Optional,
    Type,
)

import attr

import dl_formula as package
from dl_formula.connectors.base.connector import FormulaConnector
from dl_formula.connectors.registration import CONN_REG_FORMULA
from dl_utils.entrypoints import EntrypointClassManager


_CONNECTOR_EP_GROUP = f"{package.__name__}.connectors"


@attr.s
class FormulaConnectorEntrypointManager(EntrypointClassManager[FormulaConnector]):
    entrypoint_group_name = attr.ib(init=False, default=_CONNECTOR_EP_GROUP)


def _get_all_ep_connectors(ep_filter: Optional[Collection[str]] = None) -> dict[str, Type[FormulaConnector]]:
    ep_mgr = FormulaConnectorEntrypointManager()
    return ep_mgr.get_all_ep_classes(ep_filter)


def load_all_connectors() -> None:
    _get_all_ep_connectors()


def register_all_connectors(connector_ep_names: Optional[Collection[str]] = None) -> None:
    for ep_name, connector_cls in sorted(_get_all_ep_connectors(connector_ep_names).items()):
        CONN_REG_FORMULA.register_connector(connector_cls)
