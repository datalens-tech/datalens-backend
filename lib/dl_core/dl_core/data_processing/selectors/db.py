from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Optional,
    Sequence,
)

import attr

from dl_constants.enums import DataSourceRole
from dl_core.connection_executors import ConnExecutorQuery
from dl_core.data_processing.selectors.dataset_base import DatasetDataSelectorAsyncBase
from dl_core.data_processing.selectors.utils import select_data_context
from dl_core.data_processing.streaming import (
    AsyncChunked,
    AsyncChunkedBase,
    LazyAsyncChunked,
)
import dl_core.exc as exc
from dl_core.us_connection_base import ExecutorBasedMixin


if TYPE_CHECKING:
    from dl_constants.types import TBIDataValue
    from dl_core.data_processing.selectors.base import BIQueryExecutionContext


@attr.s
class DatasetDbDataSelectorAsync(DatasetDataSelectorAsyncBase):
    """Async selector that fetches data from the database"""
    # TODO: Merge all selector logic into data processors

    _active_queries: list[BIQueryExecutionContext] = attr.ib(init=False, factory=list)

    def post_exec(
        self,
        query_execution_ctx: BIQueryExecutionContext,
        exec_exception: Optional[Exception],
    ) -> None:
        """Lazy behaviour: since the query has not even been executed at this point, do nothing."""

    async def close(self) -> None:
        """Close all active queries"""
        while self._active_queries:
            await self._close_and_remove_active_query(query_execution_ctx=self._active_queries[-1])

    async def _close_and_remove_active_query(self, query_execution_ctx: BIQueryExecutionContext):  # type: ignore  # TODO: fix
        if query_execution_ctx in self._active_queries:
            self._active_queries.remove(query_execution_ctx)
            super().post_exec(query_execution_ctx=query_execution_ctx, exec_exception=None)

    async def execute_query_context(
        self,
        role: DataSourceRole,
        query_execution_ctx: BIQueryExecutionContext,
        row_count_hard_limit: Optional[int] = None,
    ) -> Optional[AsyncChunkedBase[Sequence[TBIDataValue]]]:
        if not isinstance(query_execution_ctx.target_connection, ExecutorBasedMixin):
            raise exc.NotAvailableError(
                f"Connection {type(query_execution_ctx.target_connection).__qualname__}"
                f" does not support async data selection"
            )

        with select_data_context(role=role):
            ce_factory = self.service_registry.get_conn_executor_factory()
            ce = ce_factory.get_async_conn_executor(query_execution_ctx.target_connection)

            exec_result = await ce.execute(
                ConnExecutorQuery(
                    query=query_execution_ctx.query,
                    db_name=query_execution_ctx.target_db_name,
                    user_types=query_execution_ctx.requested_bi_types,
                    # connect_args=query_execution_ctx.connect_args,
                    debug_compiled_query=query_execution_ctx.compiled_query,
                    chunk_size=None,
                )
            )
            wrapped_result_iter = AsyncChunked(chunked_data=exec_result.result)

        async def initialize_data_stream() -> AsyncChunkedBase[list[TBIDataValue]]:
            self._active_queries.append(query_execution_ctx)
            return wrapped_result_iter  # type: ignore  # TODO: fix

        async def finalize_data_stream() -> None:
            await self._close_and_remove_active_query(query_execution_ctx=query_execution_ctx)

        return LazyAsyncChunked(initializer=initialize_data_stream, finalizer=finalize_data_stream)
