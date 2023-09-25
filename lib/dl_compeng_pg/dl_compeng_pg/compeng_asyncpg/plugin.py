from dl_compeng_pg.compeng_asyncpg.data_processor_service_asyncpg import AsyncpgCompEngService
from dl_constants.enums import ProcessorType
from dl_core.data_processors.base.plugin import DataProcessorPlugin


class AsyncpgCompengPlugin(DataProcessorPlugin):
    data_processor_type = ProcessorType.ASYNCPG
    data_processor_service_cls = AsyncpgCompEngService
