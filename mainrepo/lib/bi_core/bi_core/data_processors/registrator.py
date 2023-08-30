from typing import Type

from bi_core.data_processors.base.plugin import DataProcessorPlugin
from bi_core.aio.web_app_services.data_processing.factory import register_data_processor_service_class


class DataProcessorPluginRegistrator:
    def register_data_processor_plugin(self, data_processor_plugin_cls: Type[DataProcessorPlugin]) -> None:
        register_data_processor_service_class(
            processor_type=data_processor_plugin_cls.data_processor_type,
            data_processor_srv_cls=data_processor_plugin_cls.data_processor_service_cls,
        )


DATA_PROC_REG = DataProcessorPluginRegistrator()
