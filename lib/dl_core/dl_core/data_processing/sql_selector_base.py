import abc
import enum
import logging
from typing import (
    Any,
    AsyncIterable,
    Sequence,
)

import attr

from dl_constants.types import TJSONExt
from dl_core.connection_executors.async_base import (
    AsyncConnExecutorBase,
    AsyncExecutionResult,
)
from dl_core.connection_executors.common_base import ConnExecutorQuery
from dl_core.services_registry import ServicesRegistry
from dl_core.us_connection_base import ConnectionBase


LOGGER = logging.getLogger(__name__)


@enum.unique
class SQLSelectorEvent(enum.Enum):
    metadata = "metadata"
    row = "row"  # to be deprecated
    rowchunk = "rowchunk"
    error = "error"
    footer = "footer"


TRow = tuple[TJSONExt, ...]
TMeta = dict[str, TJSONExt]
TRowChunk = tuple[TRow, ...]
# # More correct but mypy couldn't:
# TResultEvent = Union[
#     tuple[Literal['metadata'], TMeta],
#     tuple[Literal['row'], TRow],
#     tuple[Literal['rowchunk'], tuple[TRow, ...]],
#     tuple[Literal['error'], TMeta],
#     tuple[Literal['footer'], TMeta],
# ]
TResultEvent = tuple[str, TMeta | TRow | TRowChunk]
TResultEvents = AsyncIterable[TResultEvent]


@attr.s(auto_attribs=True)
class BaseSQLSelector(abc.ABC):
    """Generic per-statement SQL execution: connection executor wiring + event emission + result postprocessing.

    Subclasses must implement `make_ce_query()`. They may override `_mutate_ce()`
    to apply executor-level configuration, and `execute()` to add connection-level
    preconditions before the orchestration kicks in.
    """

    conn: ConnectionBase
    _service_registry: ServicesRegistry

    def make_ce(self) -> AsyncConnExecutorBase:
        ce_factory = self._service_registry.get_conn_executor_factory()
        ce = ce_factory.get_async_conn_executor(self.conn)
        return self._mutate_ce(ce)

    def _mutate_ce(self, ce: AsyncConnExecutorBase) -> AsyncConnExecutorBase:
        """Override to apply executor-level configuration. Default: no-op."""
        return ce

    @abc.abstractmethod
    def make_ce_query(self) -> ConnExecutorQuery:
        """Build the `ConnExecutorQuery` for this selector's SQL."""

    @staticmethod
    async def event_gen(
        result_head: TMeta,
        result_chunks: AsyncIterable[Sequence[Any]],
        result_footer_holder: TMeta,
    ) -> TResultEvents:
        # Note: returning strings for easier jsonability.
        # Additionally, those strings are in the API output as-is.
        yield SQLSelectorEvent.metadata.value, result_head
        try:
            async for chunk in result_chunks:
                yield SQLSelectorEvent.rowchunk.value, tuple(chunk)
        except Exception as err:
            LOGGER.exception("Runtime error while fetching rows: %r", err)
            raise
        else:
            yield SQLSelectorEvent.footer.value, result_footer_holder

    def process_result(self, exec_result: AsyncExecutionResult) -> TResultEvents:
        result_head = exec_result.cursor_info
        assert result_head
        db_types = result_head.get("db_types")
        user_types = exec_result.user_types
        result_head = dict(result_head, bi_types=[], db_types=[])
        if user_types:
            result_head["bi_types"] = [bi_type.name if bi_type else None for bi_type in user_types]
        if db_types:
            result_head["db_types"] = [db_type.name if db_type else None for db_type in db_types]

        result_chunks = exec_result.result
        result_footer_holder: dict = {}  # mutable  # TODO: return footer from CE

        # Wrapping the rest in an additional function to have the code above
        # execute on call rather than on iteration.
        return self.event_gen(result_head, result_chunks, result_footer_holder)

    async def _execute(self, ce: AsyncConnExecutorBase, ce_query: ConnExecutorQuery) -> TResultEvents:
        """Simplified override point."""
        exec_result = await ce.execute(ce_query)
        return self.process_result(exec_result)

    async def execute(self) -> TResultEvents:
        ce = self.make_ce()
        ce_query = self.make_ce_query()
        return await self._execute(ce, ce_query)


async def flatten_rowchunks(events: TResultEvents) -> TResultEvents:
    """Unwrap `rowchunk` events into `row` events, leaving other events untouched.

    Used by API views that haven't migrated to the chunked-row wire protocol yet.
    Rows are coerced to tuples to match `TRow`.
    """
    async for event_name, event_data in events:
        if event_name == SQLSelectorEvent.rowchunk.value:
            assert isinstance(event_data, (tuple, list))
            for row in event_data:
                assert isinstance(row, (tuple, list))
                yield SQLSelectorEvent.row.value, row if isinstance(row, tuple) else tuple(row)
        else:
            yield event_name, event_data
