from __future__ import annotations

from typing import Type

import attr

from dl_compeng_pg.compeng_aiopg.pool_aiopg import AiopgPoolWrapper
from dl_compeng_pg.compeng_aiopg.processor_aiopg import AiopgOperationProcessor
from dl_compeng_pg.compeng_pg_base.data_processor_service_pg import CompEngPgService
from dl_compeng_pg.compeng_pg_base.pool_base import BasePgPoolWrapper
from dl_core.data_processing.processing.processor import OperationProcessorAsyncBase


@attr.s
class AiopgCompEngService(CompEngPgService[AiopgPoolWrapper]):
    def _get_pool_wrapper_cls(self) -> Type[BasePgPoolWrapper]:
        return AiopgPoolWrapper

    def get_data_processor(self) -> OperationProcessorAsyncBase:
        return AiopgOperationProcessor(pg_pool=self.pool)
