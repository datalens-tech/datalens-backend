import attr

from dl_core.connection_executors.async_base import AsyncConnExecutorBase
from dl_dashsql.typed_query.primitives import (
    TypedQueryRaw,
    TypedQueryRawResult,
)
from dl_dashsql.typed_query.processor.base import TypedQueryRawProcessorBase


@attr.s
class CEBasedTypedQueryRawProcessor(TypedQueryRawProcessorBase):
    """A simple typed query raw processor that delegates all of its logic to a connection executor"""

    async_conn_executor: AsyncConnExecutorBase = attr.ib(kw_only=True)

    async def process_typed_query_raw(self, typed_query_raw: TypedQueryRaw) -> TypedQueryRawResult:
        return await self.async_conn_executor.execute_typed_query_raw(typed_query_raw=typed_query_raw)
