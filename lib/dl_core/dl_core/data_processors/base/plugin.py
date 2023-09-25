from typing import (
    ClassVar,
    Type,
)

from dl_constants.enums import ProcessorType
from dl_core.aio.web_app_services.data_processing.data_processor import DataProcessorService


class DataProcessorPlugin:
    data_processor_type: ProcessorType
    data_processor_service_cls: ClassVar[Type[DataProcessorService]]
