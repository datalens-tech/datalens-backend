from __future__ import annotations

import abc
import asyncio
import logging
from typing import (
    TYPE_CHECKING,
    AsyncIterable,
    Awaitable,
    Callable,
    Generic,
    Optional,
    TypeVar,
    final,
)

import attr

from dl_core.connection_executors.adapters.adapter_actions.async_base import (
    AsyncDBVersionAdapterAction,
    AsyncDBVersionAdapterActionNotImplemented,
    AsyncSchemaNamesAdapterAction,
    AsyncSchemaNamesAdapterActionNotImplemented,
    AsyncTableExistsActionNotImplemented,
    AsyncTableExistsAdapterAction,
    AsyncTableInfoAdapterAction,
    AsyncTableInfoAdapterActionNotImplemented,
    AsyncTableNamesAdapterAction,
    AsyncTableNamesAdapterActionNotImplemented,
    AsyncTestAdapterAction,
    AsyncTestAdapterActionNotImplemented,
    AsyncTypedQueryActionNotImplemented,
    AsyncTypedQueryAdapterAction,
)
from dl_core.connection_executors.adapters.common_base import CommonBaseDirectAdapter
from dl_core.connection_executors.models.db_adapter_data import (
    DBAdapterQuery,
    RawSchemaInfo,
)


if TYPE_CHECKING:
    from dl_constants.types import (
        TBIChunksGen,
        TBIDataRow,
    )
    from dl_core.connection_models.common_models import (
        DBIdent,
        SchemaIdent,
        TableDefinition,
        TableIdent,
    )
    from dl_dashsql.typed_query.primitives import (
        TypedQuery,
        TypedQueryResult,
    )


LOGGER = logging.getLogger(__name__)


@attr.s
class AsyncRawExecutionResult:
    raw_cursor_info: dict = attr.ib()
    raw_chunk_generator: TBIChunksGen = attr.ib()

    async def get_all_rows(self) -> AsyncIterable[TBIDataRow]:
        async for chunk in self.raw_chunk_generator:
            for row in chunk:
                yield row


_CACHE_TV = TypeVar("_CACHE_TV")


@attr.s
class AsyncCache(Generic[_CACHE_TV]):
    _cache: dict[str, _CACHE_TV] = attr.ib(init=False, default=attr.Factory(dict))
    _lock: asyncio.Lock = attr.ib(init=False, default=attr.Factory(asyncio.Lock))

    async def get(self, key: str, generator: Callable[[str], Awaitable[_CACHE_TV]]) -> _CACHE_TV:
        async with self._lock:
            if key not in self._cache:
                self._cache[key] = await generator(key)
            return self._cache[key]

    async def clear(self, finalizer: Callable[[_CACHE_TV], Awaitable[None]]) -> None:
        async with self._lock:
            for elem in self._cache.values():
                await finalizer(elem)
        self._cache.clear()


_DBA_TV = TypeVar("_DBA_TV", bound="AsyncDBAdapter")


@attr.s
class AsyncDBAdapter(metaclass=abc.ABCMeta):
    """
    This class is a composition of implementations of various data source actions.
    Each action is defined in a separate class, which should initialized in the
    corresponding action factory method.
    """

    # Adapter action fields
    _async_db_version_action: AsyncDBVersionAdapterAction = attr.ib(init=False)
    _async_schema_names_action: AsyncSchemaNamesAdapterAction = attr.ib(init=False)
    _async_table_names_action: AsyncTableNamesAdapterAction = attr.ib(init=False)
    _async_test_action: AsyncTestAdapterAction = attr.ib(init=False)
    _async_table_info_action: AsyncTableInfoAdapterAction = attr.ib(init=False)
    _async_table_exists_action: AsyncTableExistsAdapterAction = attr.ib(init=False)
    _async_typed_query_action: AsyncTypedQueryAdapterAction = attr.ib(init=False)

    def __attrs_post_init__(self) -> None:
        self._initialize_actions()

    def _initialize_actions(self):
        self._async_db_version_action = self._make_async_db_version_action()
        self._async_schema_names_action = self._make_async_schema_names_action()
        self._async_table_names_action = self._make_async_table_names_action()
        self._async_test_action = self._make_async_test_action()
        self._async_table_info_action = self._make_async_table_info_action()
        self._async_table_exists_action = self._make_async_table_exists_action()
        self._async_typed_query_action = self._make_async_typed_query_action()

    # Action factory methods

    def _make_async_db_version_action(self) -> AsyncDBVersionAdapterAction:
        # Redefine this method to enable `get_db_version`
        return AsyncDBVersionAdapterActionNotImplemented()

    def _make_async_schema_names_action(self) -> AsyncSchemaNamesAdapterAction:
        # Redefine this method to enable `get_schema_names`
        return AsyncSchemaNamesAdapterActionNotImplemented()

    def _make_async_table_names_action(self) -> AsyncTableNamesAdapterAction:
        # Redefine this method to enable `get_table_names`
        return AsyncTableNamesAdapterActionNotImplemented()

    def _make_async_test_action(self) -> AsyncTestAdapterAction:
        # Redefine this method to enable `test`
        return AsyncTestAdapterActionNotImplemented()

    def _make_async_table_info_action(self) -> AsyncTableInfoAdapterAction:
        # Redefine this method to enable `get_table_info`
        return AsyncTableInfoAdapterActionNotImplemented()

    def _make_async_table_exists_action(self) -> AsyncTableExistsAdapterAction:
        # Redefine this method to enable `is_table_exists`
        return AsyncTableExistsActionNotImplemented()

    def _make_async_typed_query_action(self) -> AsyncTypedQueryAdapterAction:
        # Redefine this method to enable `execute_typedQuery`
        return AsyncTypedQueryActionNotImplemented()

    async def test(self) -> None:
        return await self._async_test_action.run_test_action()

    async def execute_typed_query(self, typed_query: TypedQuery) -> TypedQueryResult:
        return await self._async_typed_query_action.run_typed_query_action(typed_query=typed_query)

    @abc.abstractmethod
    async def execute(self, query: DBAdapterQuery) -> AsyncRawExecutionResult:  # TODO: Implement via action
        pass

    async def get_db_version(self, db_ident: DBIdent) -> Optional[str]:
        return await self._async_db_version_action.run_db_version_action(db_ident=db_ident)

    async def get_schema_names(self, db_ident: DBIdent) -> list[str]:
        return await self._async_schema_names_action.run_schema_names_action(db_ident=db_ident)

    async def get_tables(self, schema_ident: SchemaIdent) -> list[TableIdent]:
        return await self._async_table_names_action.run_table_names_action(schema_ident=schema_ident)

    async def get_table_info(self, table_def: TableDefinition, fetch_idx_info: bool) -> RawSchemaInfo:
        return await self._async_table_info_action.run_table_info_action(
            table_def=table_def,
            fetch_idx_info=fetch_idx_info,
        )

    async def is_table_exists(self, table_ident: TableIdent) -> bool:
        return await self._async_table_exists_action.run_table_exists_action(table_ident=table_ident)

    @abc.abstractmethod
    async def close(self) -> None:
        pass

    async def __aenter__(self: _DBA_TV) -> _DBA_TV:
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):  # type: ignore  # 2024-01-30 # TODO: Function is missing a type annotation  [no-untyped-def]
        await self.close()


class AsyncDirectDBAdapter(AsyncDBAdapter, CommonBaseDirectAdapter, metaclass=abc.ABCMeta):
    pass
