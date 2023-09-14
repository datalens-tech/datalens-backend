from typing import (
    Collection,
    Optional,
)

import attr

from bi_core.core_connectors import (
    load_all_connectors,
    register_all_connectors,
)
from bi_core.core_data_processors import register_all_data_processor_plugins


@attr.s(frozen=True)
class CoreLibraryConfig:
    # Whitelist of connector entrypoints to be loaded
    core_connector_ep_names: Optional[Collection[str]] = attr.ib(kw_only=True, default=None)


def preload_bi_core() -> None:
    """Loads all entrypoint connectors without registering them"""

    load_all_connectors()


def load_bi_core(core_lib_config: CoreLibraryConfig = CoreLibraryConfig()) -> None:
    """Initialize the library"""
    register_all_connectors(connector_ep_names=core_lib_config.core_connector_ep_names)
    register_all_data_processor_plugins()
