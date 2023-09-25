from __future__ import annotations

from typing import Generator

import pytest

from dl_core.data_processing.streaming import (
    AsyncChunked,
    AsyncChunkedLimited,
    LazyAsyncChunked,
)
import dl_core.exc as exc


def chunked_range(size: int, chunk_size: int) -> Generator[Generator[int, None, None], None, None]:
    for chunk_start in range(0, size, chunk_size):
        yield range(chunk_start, min(chunk_start + chunk_size, size))


@pytest.mark.asyncio
async def test_async_chunked():
    # chunks
    chunked = AsyncChunked.from_chunked_iterable(chunked_range(97, 10))
    data = [item async for chunk in chunked.chunks for item in chunk]
    assert data == list(range(97))

    # items
    chunked = AsyncChunked.from_chunked_iterable(chunked_range(97, 10))
    data = [item async for item in chunked.items]
    assert data == list(range(97))

    # all
    chunked = AsyncChunked.from_chunked_iterable(chunked_range(97, 10))
    data = await chunked.all()
    assert data == list(range(97))


@pytest.mark.asyncio
async def test_async_chunked_limited():
    # chunks
    chunked = AsyncChunkedLimited(chunked=AsyncChunked.from_chunked_iterable(chunked_range(97, 10)), max_count=53)
    data = []
    with pytest.raises(exc.ResultRowCountLimitExceeded):
        async for chunk in chunked.chunks:
            for item in chunk:
                data.append(item)
    # Data is appended in whole chunks,
    # so the chunk that reaches the limit is never returned,
    # which is why we have only 50
    assert data == list(range(50))

    # items
    chunked = AsyncChunkedLimited(chunked=AsyncChunked.from_chunked_iterable(chunked_range(97, 10)), max_count=53)
    data = []
    with pytest.raises(exc.ResultRowCountLimitExceeded):
        async for item in chunked.items:
            data.append(item)
    assert data == list(range(53))

    # all
    chunked = AsyncChunkedLimited(chunked=AsyncChunked.from_chunked_iterable(chunked_range(97, 10)), max_count=53)
    with pytest.raises(exc.ResultRowCountLimitExceeded):
        await chunked.all()


@pytest.mark.asyncio
async def test_async_chunked_lazy():
    started_cnt = 0
    finished_cnt = 0

    async def make_range() -> AsyncChunked[int]:
        nonlocal started_cnt
        started_cnt += 1
        return AsyncChunked.from_chunked_iterable(chunked_range(97, 10))

    async def finalize() -> None:
        nonlocal finished_cnt
        finished_cnt += 1

    # chunks
    chunked = LazyAsyncChunked(initializer=make_range, finalizer=finalize)
    assert started_cnt == 0
    data = [item async for chunk in chunked.chunks for item in chunk]
    assert data == list(range(97))
    assert started_cnt == finished_cnt == 1

    # items
    chunked = LazyAsyncChunked(initializer=make_range, finalizer=finalize)
    assert started_cnt == 1
    data = [item async for item in chunked.items]
    assert data == list(range(97))
    assert started_cnt == finished_cnt == 2

    # all
    chunked = LazyAsyncChunked(initializer=make_range, finalizer=finalize)
    assert started_cnt == 2
    data = await chunked.all()
    assert data == list(range(97))
    assert started_cnt == finished_cnt == 3
