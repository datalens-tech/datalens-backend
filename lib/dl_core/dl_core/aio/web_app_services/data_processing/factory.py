
from dl_constants.enums import ProcessorType
from dl_core.aio.web_app_services.data_processing.data_processor import (
    DataProcessorConfig,
    DataProcessorService,
)


def make_compeng_service(processor_type: ProcessorType, config: DataProcessorConfig) -> DataProcessorService:
    data_proc_cls = get_data_processor_service_class(processor_type=processor_type)
    return data_proc_cls.from_config(config)


_DATA_PROCESSOR_SERVICE_CLASSES: dict[ProcessorType, type[DataProcessorService]] = {}


def register_data_processor_service_class(
    processor_type: ProcessorType,
    data_processor_srv_cls: type[DataProcessorService],
) -> None:
    try:
        assert _DATA_PROCESSOR_SERVICE_CLASSES[processor_type] is data_processor_srv_cls
    except KeyError:
        _DATA_PROCESSOR_SERVICE_CLASSES[processor_type] = data_processor_srv_cls


def get_data_processor_service_class(processor_type: ProcessorType) -> type[DataProcessorService]:
    try:
        return _DATA_PROCESSOR_SERVICE_CLASSES[processor_type]
    except KeyError:
        # TODO: Support non-pg data processor here too
        raise ValueError(f"Data processor type {processor_type} is not supported") from None
