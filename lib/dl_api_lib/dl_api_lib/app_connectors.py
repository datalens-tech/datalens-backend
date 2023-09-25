from typing import (
    Collection,
    Optional,
    Type,
)

import attr

from dl_api_connector.connector import ApiConnector
import dl_api_lib as package
from dl_api_lib.connector_registrator import CONN_REG_BI_API
from dl_utils.entrypoints import EntrypointClassManager


def _register_connector(connector_cls: Type[ApiConnector]) -> None:
    CONN_REG_BI_API.register_connector(connector_cls)


_CONNECTOR_EP_GROUP = f"{package.__name__}.connectors"


@attr.s
class ApiConnectorEntrypointManager(EntrypointClassManager[ApiConnector]):
    entrypoint_group_name = attr.ib(init=False, default=_CONNECTOR_EP_GROUP)


def register_all_connectors(connector_ep_names: Optional[Collection[str]] = None) -> None:
    connectors = ApiConnectorEntrypointManager().get_all_ep_classes()
    for ep_name, connector_cls in sorted(connectors.items()):
        if connector_ep_names is not None and ep_name not in connector_ep_names:
            continue
        CONN_REG_BI_API.register_connector(connector_cls)
