from __future__ import annotations

import asyncio
import functools
from typing import (
    TYPE_CHECKING,
    Any,
    Awaitable,
    Callable,
    Generator,
    List,
    Optional,
    Type,
    TypeVar,
    overload,
)

import attr
from typing_extensions import Literal

from dl_api_commons.base_models import RequestContextInfo
from dl_core.connection_executors import ConnExecutorQuery
from dl_core.connection_executors.adapters.adapters_base import SyncDirectDBAdapter
from dl_core.connection_executors.async_base import (
    AsyncConnExecutorBase,
    AsyncExecutionResult,
)
from dl_core.connection_executors.async_sa_executors import DefaultSqlAlchemyConnExecutor
from dl_core.connection_executors.models.db_adapter_data import (
    ExecutionStepCursorInfo,
    ExecutionStepDataChunk,
)
from dl_core.connection_executors.sync_base import (
    SyncConnExecutorBase,
    SyncExecutionResult,
)
from dl_core.db import SchemaInfo


if TYPE_CHECKING:
    from dl_constants.types import TBIDataTable
    from dl_core.connection_models.common_models import (
        DBIdent,
        SchemaIdent,
        TableDefinition,
        TableIdent,
    )
    from dl_core.connection_models.dto_defs import ConnDTO
    from dl_dashsql.typed_query.primitives import (
        TypedQuery,
        TypedQueryResult,
    )


_RET_TV = TypeVar("_RET_TV")


def init_required(wrapped: Callable[..., _RET_TV]) -> Callable[..., _RET_TV]:
    @functools.wraps(wrapped)
    def wrapper(self: "SyncWrapperForAsyncConnExecutor", *args: Any, **kwargs: Any) -> _RET_TV:
        if not self.is_initialized:
            self.initialize()
        return wrapped(self, *args, **kwargs)

    return wrapper


_CORO_RET_TV = TypeVar("_CORO_RET_TV")


@attr.s(cmp=False, hash=False)
class SyncWrapperForAsyncConnExecutor(SyncConnExecutorBase):
    _async_conn_executor: AsyncConnExecutorBase = attr.ib()
    _loop: asyncio.AbstractEventLoop = attr.ib()

    def _await_sync(self, coro: Awaitable[_CORO_RET_TV]) -> _CORO_RET_TV:
        return self._loop.run_until_complete(coro)

    def is_conn_dto_equals(self, another: ConnDTO) -> bool:
        return self._async_conn_executor.is_conn_dto_equals(another)

    def is_context_info_equals(self, another: RequestContextInfo) -> bool:
        return self._async_conn_executor.is_context_info_equals(another)

    @property
    def is_initialized(self) -> bool:
        return self._async_conn_executor.is_initialized

    def initialize(self) -> None:
        self._await_sync(self._async_conn_executor.initialize())

    def close(self) -> None:
        sa_adapter = self._extract_sync_sa_adapter(raise_on_not_exists=False)

        if sa_adapter is not None:
            sa_adapter.close()

        self._await_sync(self._async_conn_executor.close())

    @overload
    def _extract_sync_sa_adapter(self, raise_on_not_exists: Literal[False]) -> Optional[SyncDirectDBAdapter]:
        pass

    @overload  # noqa
    def _extract_sync_sa_adapter(self, raise_on_not_exists: Literal[True]) -> SyncDirectDBAdapter:
        pass

    def _extract_sync_sa_adapter(self, raise_on_not_exists: bool = False) -> Optional[SyncDirectDBAdapter]:  # noqa
        actual_executor = self._async_conn_executor
        if not actual_executor.is_initialized:
            self.initialize()

        if isinstance(actual_executor, DefaultSqlAlchemyConnExecutor):
            # noinspection PyProtectedMember
            sa_adapter = actual_executor._get_sync_sa_adapter()
            if sa_adapter is None and raise_on_not_exists:
                raise ValueError(f"Async conn executor {actual_executor} has no sync SA adapter.")
            return sa_adapter

        if raise_on_not_exists:
            raise TypeError("Direct sync execution allowed only if wrapping DefaultSqlAlchemyConnExecutor")

        return None

    def _execute_on_sync_adapter_from_wrapped_executor(self, query: ConnExecutorQuery) -> SyncExecutionResult:
        sa_adapter = self._extract_sync_sa_adapter(raise_on_not_exists=True)
        db_adapter_query = self._async_conn_executor.executor_query_to_db_adapter_query(query)
        generator = sa_adapter.execute_by_steps(db_adapter_query)
        cursor_msg = next(generator)

        if not isinstance(cursor_msg, ExecutionStepCursorInfo):
            raise ValueError(f"Unexpected first message from SA adapter: {cursor_msg}")

        def data_generator() -> Generator[TBIDataTable, None, None]:
            for msg in generator:
                if not isinstance(msg, ExecutionStepDataChunk):
                    raise ValueError(f"Unexpected message instead of ExecutionStepDataChunk from SA adapter : {msg}")
                # TODO CONSIDER: Maybe convert to user types in SAAdapter
                yield [self._async_conn_executor.cast_row_to_output(row, query.user_types) for row in msg.chunk]

        return SyncExecutionResult(cursor_info=cursor_msg.cursor_info, result=data_generator())

    def _execute_typed_query_on_sync_adapter_from_wrapped_executor(self, typed_query: TypedQuery) -> TypedQueryResult:
        sa_adapter = self._extract_sync_sa_adapter(raise_on_not_exists=True)
        result = sa_adapter.execute_typed_query(typed_query=typed_query)
        return result

    def _convert_async_result_to_sync(self, async_result: AsyncExecutionResult) -> SyncExecutionResult:
        def data_generator() -> Generator[TBIDataTable, None, None]:
            chunk_iter = async_result.result.__aiter__()
            while True:
                try:
                    yield self._await_sync(chunk_iter.__anext__())
                except StopAsyncIteration:
                    return

        return SyncExecutionResult(cursor_info=async_result.cursor_info, result=data_generator())

    def _execute_in_loop(self, query: ConnExecutorQuery) -> SyncExecutionResult:
        async_result: AsyncExecutionResult = self._await_sync(self._async_conn_executor.execute(query))
        return self._convert_async_result_to_sync(async_result)

    def _execute_typed_query_in_loop(self, typed_query: TypedQuery) -> TypedQueryResult:
        result: TypedQueryResult = self._await_sync(
            self._async_conn_executor.execute_typed_query(typed_query),
        )
        return result

    @init_required
    def execute(self, query: ConnExecutorQuery) -> SyncExecutionResult:
        sa_adapter = self._extract_sync_sa_adapter(raise_on_not_exists=False)
        if sa_adapter is not None:
            return self._execute_on_sync_adapter_from_wrapped_executor(query)
        return self._execute_in_loop(query)

    @init_required
    def execute_typed_query(self, typed_query: TypedQuery) -> TypedQueryResult:
        sa_adapter = self._extract_sync_sa_adapter(raise_on_not_exists=False)
        if sa_adapter is not None:
            return self._execute_typed_query_on_sync_adapter_from_wrapped_executor(typed_query)
        return self._execute_typed_query_in_loop(typed_query)

    @init_required
    def test(self) -> None:
        sa_adapter = self._extract_sync_sa_adapter(raise_on_not_exists=False)
        if sa_adapter is not None:
            return sa_adapter.test()
        return self._await_sync(self._async_conn_executor.test())

    @init_required
    def get_db_version(self, db_ident: DBIdent) -> Optional[str]:
        sa_adapter = self._extract_sync_sa_adapter(raise_on_not_exists=False)
        if sa_adapter is not None:
            return sa_adapter.get_db_version(db_ident)
        return self._await_sync(self._async_conn_executor.get_db_version(db_ident))

    @init_required
    def get_schema_names(self, db_ident: DBIdent) -> List[str]:
        sa_adapter = self._extract_sync_sa_adapter(raise_on_not_exists=False)
        if sa_adapter is not None:
            return sa_adapter.get_schema_names(db_ident)
        return self._await_sync(self._async_conn_executor.get_schema_names(db_ident))

    @init_required
    def get_tables(self, schema_ident: SchemaIdent) -> List[TableIdent]:
        sa_adapter = self._extract_sync_sa_adapter(raise_on_not_exists=False)
        if sa_adapter is not None:
            return sa_adapter.get_tables(schema_ident)
        return self._await_sync(self._async_conn_executor.get_tables(schema_ident))

    @init_required
    def get_table_schema_info(self, table_def: TableDefinition) -> SchemaInfo:
        sa_adapter = self._extract_sync_sa_adapter(raise_on_not_exists=False)

        if sa_adapter is not None:
            raw_schema_info = sa_adapter.get_table_info(
                table_def,
                self._async_conn_executor._conn_options.fetch_table_indexes,
            )
            return self._async_conn_executor.create_schema_info_from_raw_schema_info(raw_schema_info)

        return self._await_sync(self._async_conn_executor.get_table_schema_info(table_def))

    @init_required
    def is_table_exists(self, table_ident: TableIdent) -> bool:
        sa_adapter = self._extract_sync_sa_adapter(raise_on_not_exists=False)
        if sa_adapter is not None:
            return sa_adapter.is_table_exists(table_ident)
        return self._await_sync(self._async_conn_executor.is_table_exists(table_ident))

    @init_required
    def __enter__(self) -> SyncWrapperForAsyncConnExecutor:
        return self

    def __exit__(self, exc_type: Optional[Type[Exception]], exc_val: Optional[Exception], exc_tb: Any) -> None:
        self.close()
