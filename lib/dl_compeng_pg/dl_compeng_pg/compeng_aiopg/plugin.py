from dl_compeng_pg.compeng_aiopg.data_processor_service_aiopg import AiopgCompEngService
from dl_constants.enums import ProcessorType
from dl_core.data_processors.base.plugin import DataProcessorPlugin


class AiopgCompengPlugin(DataProcessorPlugin):
    data_processor_type = ProcessorType.AIOPG
    data_processor_service_cls = AiopgCompEngService
