from __future__ import annotations

import logging
from typing import (
    TYPE_CHECKING,
    List,
    Optional,
    Sequence,
)

import attr

from dl_constants.enums import DataSourceRole
from dl_core.data_processing.selectors.base import BIQueryExecutionContext
from dl_core.data_processing.selectors.db import DatasetDbDataSelectorAsync
from dl_core.data_processing.streaming import (
    AsyncChunked,
    AsyncChunkedBase,
    LazyAsyncChunked,
)


if TYPE_CHECKING:
    from dl_constants.types import TBIDataValue


LOGGER = logging.getLogger(__name__)


@attr.s
class LazyCachedDatasetDataSelectorAsync(DatasetDbDataSelectorAsync):
    """
    Lazy asynchronous cached dataset data selector
    """

    _active_queries: List[BIQueryExecutionContext] = attr.ib(init=False, factory=list)

    # pre_exec is not redefined so that a reporting record is created
    # even if the selector doesn't fetch any data in the end

    def post_exec(
        self,
        query_execution_ctx: BIQueryExecutionContext,
        exec_exception: Optional[Exception],
    ) -> None:
        """Lazy behaviour: since the query has not even been executed at this point, do nothing."""

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
        result_iter_awaitable = super().execute_query_context(
            role=role, query_execution_ctx=query_execution_ctx, row_count_hard_limit=row_count_hard_limit
        )

        async def initialize_data_stream() -> AsyncChunkedBase[List[TBIDataValue]]:
            wrapped_result_iter = await result_iter_awaitable
            if wrapped_result_iter is None:
                wrapped_result_iter = AsyncChunked.from_chunked_iterable([[]])
            self._active_queries.append(query_execution_ctx)
            return wrapped_result_iter  # type: ignore  # TODO: fix

        async def finalize_data_stream() -> None:
            await self._close_and_remove_active_query(query_execution_ctx=query_execution_ctx)

        return LazyAsyncChunked(initializer=initialize_data_stream, finalizer=finalize_data_stream)  # type: ignore  # TODO: fix

    async def close(self) -> None:
        """Close all active queries"""
        while self._active_queries:
            await self._close_and_remove_active_query(query_execution_ctx=self._active_queries[-1])
