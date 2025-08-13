from typing import (
    Collection,
    Optional,
)

import attr

import dl_core as package
from dl_core.connectors.base.connector import CoreConnector
from dl_utils.entrypoints import EntrypointClassManager


_CONNECTOR_EP_GROUP = f"{package.__name__}.connectors"


@attr.s
class CoreConnectorEntrypointManager(EntrypointClassManager[CoreConnector]):
    entrypoint_group_name = attr.ib(init=False, default=_CONNECTOR_EP_GROUP)


def get_all_connectors(ep_filter: Optional[Collection[str]] = None) -> dict[str, type[CoreConnector]]:
    ep_mgr = CoreConnectorEntrypointManager()
    return ep_mgr.get_all_ep_classes(ep_filter)


def get_connector_cls(name: str) -> type[CoreConnector]:
    ep_mgr = CoreConnectorEntrypointManager()
    return ep_mgr.get_ep_class(name=name)
