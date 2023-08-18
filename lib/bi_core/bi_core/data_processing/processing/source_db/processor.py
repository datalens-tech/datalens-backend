from __future__ import annotations

from typing import ClassVar, Dict, Optional, Type

import attr

from bi_constants.enums import DataSourceRole

from bi_core.data_processing.processing.operation import BaseOp, DownloadOp, CalcOp, JoinOp
from bi_core.data_processing.processing.context import OpExecutionContext
from bi_core.data_processing.processing.db_base.exec_adapter_base import ProcessorDbExecAdapterBase
from bi_core.data_processing.processing.db_base.op_executors import (
    OpExecutorAsync, DownloadOpExecutorAsync, CalcOpExecutorAsync, JoinOpExecutorAsync
)
from bi_core.data_processing.processing.source_db.selector_exec_adapter import SourceDbExecAdapter
from bi_core.data_processing.stream_base import AbstractStream
from bi_core.data_processing.processing.processor import OperationProcessorAsyncBase
from bi_core.data_processing.selectors.base import DataSelectorAsyncBase
from bi_core.us_dataset import Dataset
from bi_core.us_manager.local_cache import USEntryBuffer


@attr.s
class SourceDbOperationProcessor(OperationProcessorAsyncBase):

    _role: DataSourceRole = attr.ib(kw_only=True)
    _dataset: Dataset = attr.ib(kw_only=True)
    _selector: DataSelectorAsyncBase = attr.ib(kw_only=True)
    _row_count_hard_limit: Optional[int] = attr.ib(kw_only=True, default=None)
    _us_entry_buffer: USEntryBuffer = attr.ib(kw_only=True)

    _db_ex_adapter: Optional[ProcessorDbExecAdapterBase] = attr.ib(init=False, default=None)

    _executors: ClassVar[Dict[Type[BaseOp], Type[OpExecutorAsync]]] = {
        DownloadOp: DownloadOpExecutorAsync,
        CalcOp: CalcOpExecutorAsync,
        JoinOp: JoinOpExecutorAsync,
    }

    async def ping(self) -> Optional[int]:
        return 1

    async def start(self) -> None:
        self._db_ex_adapter = SourceDbExecAdapter(
            role=self._role,
            dataset=self._dataset,
            selector=self._selector,
            row_count_hard_limit=self._row_count_hard_limit,
            us_entry_buffer=self._us_entry_buffer,
        )

    async def stop(self) -> None:
        self._db_ex_adapter = None

    async def execute_operation(self, op: BaseOp, ctx: OpExecutionContext) -> AbstractStream:
        opex_cls: Type[OpExecutorAsync] = self._executors[type(op)]
        opex = opex_cls(db_ex_adapter=self._db_ex_adapter, ctx=ctx)  # type: ignore  # TODO: fix
        return await opex.execute(op)
