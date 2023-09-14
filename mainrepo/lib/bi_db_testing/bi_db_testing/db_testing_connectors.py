from importlib import metadata
from typing import Type

import bi_db_testing as package
from bi_db_testing.connector_registration import CONN_REG_DB_TESTING
from bi_db_testing.connectors.base.connector import DbTestingConnector

_CONNECTOR_EP_GROUP = f"{package.__name__}.connectors"


def _get_all_ep_connectors() -> dict[str, Type[DbTestingConnector]]:
    entrypoints = list(metadata.entry_points().select(group=_CONNECTOR_EP_GROUP))  # type: ignore
    return {ep.name: ep.load() for ep in entrypoints}


def register_all_connectors() -> None:
    connectors = _get_all_ep_connectors()
    for _, connector_cls in sorted(connectors.items()):
        CONN_REG_DB_TESTING.register_connector(connector_cls)
