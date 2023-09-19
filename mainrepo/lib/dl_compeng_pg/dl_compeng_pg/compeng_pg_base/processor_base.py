from __future__ import annotations

import abc
from typing import (
    ClassVar,
    Dict,
    Generic,
    Optional,
    Type,
    TypeVar,
)

import attr

from dl_compeng_pg.compeng_pg_base.exec_adapter_base import PostgreSQLExecAdapterAsync
from dl_compeng_pg.compeng_pg_base.op_executors import UploadOpExecutorAsync
from dl_compeng_pg.compeng_pg_base.pool_base import BasePgPoolWrapper
from dl_constants.enums import BIType
from dl_core.data_processing.processing.context import OpExecutionContext
from dl_core.data_processing.processing.db_base.op_executors import (
    CalcOpExecutorAsync,
    DownloadOpExecutorAsync,
    JoinOpExecutorAsync,
    OpExecutorAsync,
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


_ADAPTER_TV = TypeVar("_ADAPTER_TV", bound=PostgreSQLExecAdapterAsync)
_POOL_TV = TypeVar("_POOL_TV", bound=BasePgPoolWrapper)
_CONN_TV = TypeVar("_CONN_TV")


@attr.s
class PostgreSQLOperationProcessor(
    OperationProcessorAsyncBase, Generic[_ADAPTER_TV, _POOL_TV, _CONN_TV], metaclass=abc.ABCMeta
):
    _pg_pool: _POOL_TV = attr.ib()
    _task_timeout: Optional[int] = attr.ib(default=None)
    _pgex_adapter: Optional[_ADAPTER_TV] = attr.ib(init=False, default=None)
    _pg_conn: Optional[_CONN_TV] = attr.ib(init=False, default=None)

    @abc.abstractmethod
    async def start(self) -> None:
        """Prepare for work."""

    @abc.abstractmethod
    async def end(self) -> None:
        """Cleanup."""

    async def ping(self) -> Optional[int]:
        assert self._pgex_adapter is not None
        result = await self._pgex_adapter.scalar("select 1", user_type=BIType.integer)
        assert result is None or isinstance(result, int)
        return result

    _executors: ClassVar[Dict[Type[BaseOp], Type[OpExecutorAsync]]] = {
        UploadOp: UploadOpExecutorAsync,
        DownloadOp: DownloadOpExecutorAsync,
        CalcOp: CalcOpExecutorAsync,
        JoinOp: JoinOpExecutorAsync,
    }

    async def execute_operation(self, op: BaseOp, ctx: OpExecutionContext) -> AbstractStream:
        opex_cls: Type[OpExecutorAsync] = self._executors[type(op)]
        assert self._pgex_adapter is not None
        opex = opex_cls(db_ex_adapter=self._pgex_adapter, ctx=ctx)  # type: ignore  # TODO: fix
        return await opex.execute(op)
