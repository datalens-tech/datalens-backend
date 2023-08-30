from typing import Collection, Optional, Type

import attr

from bi_utils.entrypoints import EntrypointClassManager

from bi_api_connector.connector import BiApiConnector

import bi_api_lib as package
from bi_api_lib.connector_registrator import CONN_REG_BI_API
from bi_api_lib.manual_connector_registration import register_non_connectorized_source_schemas


def _register_connector(connector_cls: Type[BiApiConnector]) -> None:
    CONN_REG_BI_API.register_connector(connector_cls)


_CONNECTOR_EP_GROUP = f'{package.__name__}.connectors'


@attr.s
class BiApiConnectorEntrypointManager(EntrypointClassManager[BiApiConnector]):
    entrypoint_group_name = attr.ib(init=False, default=_CONNECTOR_EP_GROUP)


def register_all_connectors(connector_ep_names: Optional[Collection[str]] = None) -> None:
    connectors = BiApiConnectorEntrypointManager().get_all_ep_classes()
    for ep_name, connector_cls in sorted(connectors.items()):
        if connector_ep_names is not None and ep_name not in connector_ep_names:
            continue
        CONN_REG_BI_API.register_connector(connector_cls)

    register_non_connectorized_source_schemas()  # FIXME: Remove
