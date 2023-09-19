from typing import Type

import attr

import dl_core as package
from dl_core.data_processors.base.plugin import DataProcessorPlugin
from dl_utils.entrypoints import EntrypointClassManager


_DATA_PROC_SRV_EP_GROUP = f"{package.__name__}.data_processor_plugins"


@attr.s
class DataProcessorPluginEntrypointManager(EntrypointClassManager[DataProcessorPlugin]):
    entrypoint_group_name = attr.ib(init=False, default=_DATA_PROC_SRV_EP_GROUP)


def get_all_data_processor_plugins() -> dict[str, Type[DataProcessorPlugin]]:
    ep_mgr = DataProcessorPluginEntrypointManager()
    return ep_mgr.get_all_ep_classes()
