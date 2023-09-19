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
)

import attr

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


class AsyncDBAdapter(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def test(self) -> None:
        pass

    @abc.abstractmethod
    async def execute(self, query: DBAdapterQuery) -> AsyncRawExecutionResult:
        pass

    @abc.abstractmethod
    async def get_db_version(self, db_ident: DBIdent) -> Optional[str]:
        pass

    @abc.abstractmethod
    async def get_schema_names(self, db_ident: DBIdent) -> list[str]:
        pass

    @abc.abstractmethod
    async def get_tables(self, schema_ident: SchemaIdent) -> list[TableIdent]:
        pass

    @abc.abstractmethod
    async def get_table_info(self, table_def: TableDefinition, fetch_idx_info: bool) -> RawSchemaInfo:
        pass

    @abc.abstractmethod
    async def is_table_exists(self, table_ident: TableIdent) -> bool:
        pass

    @abc.abstractmethod
    async def close(self) -> None:
        pass

    async def __aenter__(self: _DBA_TV) -> _DBA_TV:
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):  # type: ignore
        await self.close()


class AsyncDirectDBAdapter(AsyncDBAdapter, CommonBaseDirectAdapter, metaclass=abc.ABCMeta):
    pass
