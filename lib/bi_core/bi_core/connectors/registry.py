from typing import Type

import attr

from bi_utils.entrypoints import EntrypointClassManager

import bi_core as package
from bi_core.connectors.base.connector import CoreConnector


_CONNECTOR_EP_GROUP = f'{package.__name__}.connectors'


@attr.s
class CoreConnectorEntrypointManager(EntrypointClassManager[CoreConnector]):
    entrypoint_group_name = attr.ib(init=False, default=_CONNECTOR_EP_GROUP)


def get_all_connectors() -> dict[str, Type[CoreConnector]]:
    ep_mgr = CoreConnectorEntrypointManager()
    return ep_mgr.get_all_ep_classes()


def get_connector_cls(name: str) -> Type[CoreConnector]:
    ep_mgr = CoreConnectorEntrypointManager()
    return ep_mgr.get_ep_class(name=name)
