from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Optional,
    Sequence,
)

import attr

from bi_constants.enums import DataSourceRole
from bi_core.connection_executors import ConnExecutorQuery
from bi_core.data_processing.selectors.dataset_base import DatasetDataSelectorAsyncBase
from bi_core.data_processing.selectors.utils import select_data_context
from bi_core.data_processing.streaming import (
    AsyncChunked,
    AsyncChunkedBase,
)
import bi_core.exc as exc
from bi_core.us_connection_base import ExecutorBasedMixin

if TYPE_CHECKING:
    from bi_constants.types import TBIDataValue
    from bi_core.data_processing.selectors.base import BIQueryExecutionContext


@attr.s
class DatasetDbDataSelectorAsync(DatasetDataSelectorAsyncBase):
    """Async selector that fetches data from the database"""

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
            return AsyncChunked(chunked_data=exec_result.result)
