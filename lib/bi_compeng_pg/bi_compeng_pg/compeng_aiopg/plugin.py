from bi_constants.enums import ProcessorType

from bi_core.data_processors.base.plugin import DataProcessorPlugin
from bi_compeng_pg.compeng_aiopg.data_processor_service_aiopg import AiopgCompEngService


class AiopgCompengPlugin(DataProcessorPlugin):
    data_processor_type = ProcessorType.AIOPG
    data_processor_service_cls = AiopgCompEngService
