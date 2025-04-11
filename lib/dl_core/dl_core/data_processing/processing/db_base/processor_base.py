from __future__ import annotations

from typing import (
    ClassVar,
    Optional,
)

import attr

from dl_core.data_processing.processing.context import OpExecutionContext
from dl_core.data_processing.processing.db_base.op_executors import (
    CalcOpExecutorAsync,
    DownloadOpExecutorAsync,
    JoinOpExecutorAsync,
    OpExecutorAsync,
    UploadOpExecutorAsync,
)
from dl_core.data_processing.processing.operation import (
    BaseOp,
    CalcOp,
    DownloadOp,
    JoinOp,
    UploadOp,
)
from dl_core.data_processing.processing.processor import OperationProcessorAsyncBase
from dl_core.data_processing.stream_base import AbstractStream


@attr.s
class ExecutorBasedOperationProcessor(OperationProcessorAsyncBase):
    _executors: ClassVar[dict[type[BaseOp], type[OpExecutorAsync]]] = {
        DownloadOp: DownloadOpExecutorAsync,
        CalcOp: CalcOpExecutorAsync,
        JoinOp: JoinOpExecutorAsync,
        UploadOp: UploadOpExecutorAsync,
    }

    async def ping(self) -> Optional[int]:
        return 1

    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass

    def get_executor_class(self, op_type: type[BaseOp]) -> type[OpExecutorAsync]:
        return self._executors[op_type]

    async def execute_operation(self, op: BaseOp, ctx: OpExecutionContext) -> AbstractStream:
        opex_cls: type[OpExecutorAsync] = self.get_executor_class(type(op))
        opex = opex_cls(db_ex_adapter=self.db_ex_adapter, ctx=ctx)
        return await opex.execute(op)
