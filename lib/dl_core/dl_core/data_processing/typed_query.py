import attr

from dl_core.connection_executors.async_base import AsyncConnExecutorBase
from dl_dashsql.typed_query.primitives import (
    TypedQuery,
    TypedQueryResult,
)
from dl_dashsql.typed_query.processor.base import TypedQueryProcessorBase


@attr.s
class CEBasedTypedQueryProcessor(TypedQueryProcessorBase):
    """A simple typed query processor that delegates all of its logic to a connection executor"""

    async_conn_executor: AsyncConnExecutorBase = attr.ib(kw_only=True)

    async def process_typed_query(self, typed_query: TypedQuery) -> TypedQueryResult:
        return await self.async_conn_executor.execute_typed_query(typed_query=typed_query)
