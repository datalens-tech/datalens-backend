from typing import ClassVar, Type

from bi_constants.enums import ProcessorType

from bi_core.aio.web_app_services.data_processing.data_processor import DataProcessorService


class DataProcessorPlugin:
    data_processor_type: ProcessorType
    data_processor_service_cls: ClassVar[Type[DataProcessorService]]
