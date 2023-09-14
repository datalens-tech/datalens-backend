from __future__ import annotations

from typing import Type

import attr

from bi_compeng_pg.compeng_asyncpg.pool_asyncpg import AsyncpgPoolWrapper
from bi_compeng_pg.compeng_asyncpg.processor_asyncpg import AsyncpgOperationProcessor
from bi_compeng_pg.compeng_pg_base.data_processor_service_pg import CompEngPgService
from bi_compeng_pg.compeng_pg_base.pool_base import BasePgPoolWrapper
from bi_core.data_processing.processing.processor import OperationProcessorAsyncBase


@attr.s
class AsyncpgCompEngService(CompEngPgService[AsyncpgPoolWrapper]):
    def _get_pool_wrapper_cls(self) -> Type[BasePgPoolWrapper]:
        return AsyncpgPoolWrapper

    def get_data_processor(self) -> OperationProcessorAsyncBase:
        return AsyncpgOperationProcessor(pg_pool=self.pool)
