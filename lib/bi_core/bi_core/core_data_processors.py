from typing import Type

from bi_core.data_processors.ep_loader import get_all_data_processor_plugins
from bi_core.data_processors.registrator import DATA_PROC_REG
from bi_core.data_processors.base.plugin import DataProcessorPlugin


def _register_plugin(plugin_cls: Type[DataProcessorPlugin]) -> None:
    DATA_PROC_REG.register_data_processor_plugin(plugin_cls)


def register_all_data_processor_plugins() -> None:
    for _, plugin_cls in sorted(get_all_data_processor_plugins().items()):
        _register_plugin(plugin_cls)
