from __future__ import annotations

import logging
from typing import (
    Any,
    AsyncGenerator,
    AsyncIterable,
    Awaitable,
    Callable,
    Generic,
    Iterable,
    Optional,
    Sequence,
    TypeVar,
)

import attr


LOGGER = logging.getLogger(__name__)


_ENTRY_TV = TypeVar("_ENTRY_TV")
# Alias for readability
TChunk = Sequence  # `Sequence[_ENTRY_TV]`  # Chunk of "rows" (which might be anything)


class AsyncChunkedBase(Generic[_ENTRY_TV]):
    """A wrapper for an iterable providing both chunked and pre-item interfaces"""

    @property
    def items(self) -> AsyncIterable[_ENTRY_TV]:
        raise NotImplementedError

    @property
    def chunks(self) -> AsyncIterable[TChunk[_ENTRY_TV]]:
        raise NotImplementedError

    async def all(self) -> list[_ENTRY_TV]:
        result: list[_ENTRY_TV] = []
        async for chunk in self.chunks:
            result += chunk
        return result

    def limit(self, max_count: int, limit_exception: type[Exception]) -> AsyncChunkedLimited:
        return AsyncChunkedLimited(chunked=self, max_count=max_count, limit_exc_to_raise=limit_exception)


@attr.s
class AsyncChunked(AsyncChunkedBase[_ENTRY_TV]):
    _chunked_data: AsyncIterable[TChunk[_ENTRY_TV]] = attr.ib(repr=False)

    @classmethod
    def from_chunked_iterable(
        cls,
        sync_chunked_data: Iterable[TChunk[_ENTRY_TV]],
    ) -> AsyncChunked[_ENTRY_TV]:
        async def async_chunk_gen() -> AsyncGenerator[TChunk[_ENTRY_TV], None]:
            for chunk in sync_chunked_data:
                yield chunk

        return cls(chunked_data=async_chunk_gen())

    @property
    def items(self) -> AsyncIterable[_ENTRY_TV]:
        async def item_gen() -> AsyncGenerator[_ENTRY_TV, None]:
            async for chunk in self._chunked_data:
                for item in chunk:
                    yield item

        return item_gen()

    @property
    def chunks(self) -> AsyncIterable[TChunk[_ENTRY_TV]]:
        return self._chunked_data


@attr.s
class AsyncChunkedLimited(AsyncChunkedBase[_ENTRY_TV]):
    _chunked: AsyncChunkedBase = attr.ib()
    _max_count: int = attr.ib()
    limit_exc_to_raise: type[Exception] = attr.ib(kw_only=True)

    @property
    def items(self) -> AsyncIterable[_ENTRY_TV]:
        if self._max_count is None:
            return self._chunked.items

        async def async_item_gen() -> AsyncGenerator[_ENTRY_TV, None]:
            cnt = 0
            async for item in self._chunked.items:
                cnt += 1
                if cnt > self._max_count:
                    raise self.limit_exc_to_raise()
                yield item

            LOGGER.info(f"Received {cnt} data rows.")

        return async_item_gen()

    @property
    def chunks(self) -> AsyncIterable[TChunk[_ENTRY_TV]]:
        if self._max_count is None:
            return self._chunked.chunks

        async def async_chunk_gen() -> AsyncGenerator[TChunk[_ENTRY_TV], None]:
            cnt = 0
            async for chunk in self._chunked.chunks:
                cnt += len(chunk)
                if cnt > self._max_count:
                    raise self.limit_exc_to_raise()
                yield chunk

            LOGGER.info(f"Received {cnt} data rows.")

        return async_chunk_gen()


@attr.s
class LazyAsyncChunked(AsyncChunkedBase[_ENTRY_TV]):
    """
    ``AsyncChunkedBase`` implementation that executes an initialization callable only when iteration starts.
    """

    _initializer: Callable[[], Awaitable[AsyncChunkedBase[_ENTRY_TV]]] = attr.ib(repr=False)
    _finalizer: Callable[[], Awaitable[Any]] = attr.ib(repr=False)
    _chunked: Optional[Awaitable[AsyncChunkedBase[_ENTRY_TV]]] = attr.ib(init=False, default=None)

    @property
    def items(self) -> AsyncIterable[_ENTRY_TV]:
        async def item_gen() -> AsyncGenerator[_ENTRY_TV, None]:
            if self._chunked is None:
                self._chunked = await self._initializer()  # type: ignore  # TODO: fix
            try:
                async for item in self._chunked.items:  # type: ignore  # TODO: fix
                    yield item
            finally:
                await self._finalizer()

        return item_gen()

    @property
    def chunks(self) -> AsyncIterable[TChunk[_ENTRY_TV]]:
        async def chunk_gen() -> AsyncGenerator[TChunk[_ENTRY_TV], None]:
            if self._chunked is None:
                self._chunked = await self._initializer()  # type: ignore  # TODO: fix
            try:
                async for chunk in self._chunked.chunks:  # type: ignore  # TODO: fix
                    yield chunk
            finally:
                await self._finalizer()

        return chunk_gen()


_CBO_ITEM_TV = TypeVar("_CBO_ITEM_TV")


async def chunkify_by_one(
    items: AsyncIterable[_CBO_ITEM_TV],
) -> AsyncIterable[Sequence[_CBO_ITEM_TV]]:
    """Helper to wrap an iterable into a chunked iterable with one item per chunk"""
    async for item in items:
        yield (item,)
