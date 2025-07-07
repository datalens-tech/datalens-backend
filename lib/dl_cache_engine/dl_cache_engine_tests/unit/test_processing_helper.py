"""Unit tests for processing_helper module"""

from typing import Optional

import attr
import pytest

from dl_cache_engine.primitives import (
    BIQueryCacheOptions,
    DataKeyPart,
    LocalKeyRepresentation,
)
from dl_cache_engine.processing_helper import (
    CacheProcessingHelper,
    CacheSituation,
    TJSONExtChunkStream,
)
from dl_utils.streaming import AsyncChunked


@pytest.fixture
def cache_helper() -> CacheProcessingHelper:
    return CacheProcessingHelper(cache_engine=None)


@pytest.fixture
def cache_options() -> BIQueryCacheOptions:
    key = LocalKeyRepresentation(key_parts=(DataKeyPart("test", "value"),))
    return BIQueryCacheOptions(
        cache_enabled=True,
        key=key,
        ttl_sec=60,
        refresh_ttl_on_read=False,
    )


@pytest.fixture
def disabled_cache_options() -> BIQueryCacheOptions:
    return BIQueryCacheOptions(
        cache_enabled=False,
        key=None,
        ttl_sec=0,
        refresh_ttl_on_read=False,
    )


@pytest.mark.asyncio
async def test_get_cache_entry_manager_none_when_cache_disabled(
    cache_helper: CacheProcessingHelper,
    disabled_cache_options: BIQueryCacheOptions,
) -> None:
    """Should return None when cache is disabled"""
    result = await cache_helper.get_cache_entry_manager(cache_options=disabled_cache_options)
    assert result is None


@pytest.mark.asyncio
async def test_get_cache_entry_manager_none_when_no_options(
    cache_helper: CacheProcessingHelper,
) -> None:
    """Should return None when no cache options provided"""
    result = await cache_helper.get_cache_entry_manager(cache_options=None)
    assert result is None


@pytest.mark.asyncio
async def test_get_cache_entry_manager_none_when_zero_ttl(
    cache_helper: CacheProcessingHelper,
    cache_options: BIQueryCacheOptions,
) -> None:
    """Should return None when TTL is 0"""
    options = attr.evolve(cache_options, ttl_sec=0)
    result = await cache_helper.get_cache_entry_manager(cache_options=options)
    assert result is None


@pytest.mark.asyncio
async def test_dump_error_for_cache(cache_helper: CacheProcessingHelper) -> None:
    """Should properly serialize error for cache"""
    error = ValueError("test error")
    result = cache_helper._dump_error_for_cache(error)
    assert result == "test error"


@pytest.mark.asyncio
async def test_run_with_cache_disabled(
    cache_helper: CacheProcessingHelper,
    disabled_cache_options: BIQueryCacheOptions,
) -> None:
    """Should handle disabled cache case"""

    async def generate_func() -> Optional[TJSONExtChunkStream]:
        return AsyncChunked.from_chunked_iterable([[1, 2, 3]])

    situation, result = await cache_helper.run_with_cache(
        generate_func=generate_func,
        cache_options=disabled_cache_options,
    )

    assert situation == CacheSituation.cache_disabled
    assert result is not None
    data = await result.all()
    assert data == [1, 2, 3]


@pytest.mark.asyncio
async def test_run_with_cache_error_handling(
    cache_helper: CacheProcessingHelper,
    cache_options: BIQueryCacheOptions,
) -> None:
    """Should handle errors during generation"""

    async def generate_func() -> Optional[TJSONExtChunkStream]:
        raise ValueError("test error")

    with pytest.raises(ValueError, match="test error"):
        await cache_helper.run_with_cache(
            generate_func=generate_func,
            cache_options=cache_options,
        )


@pytest.mark.asyncio
async def test_run_with_cache_none_result(
    cache_helper: CacheProcessingHelper,
    cache_options: BIQueryCacheOptions,
) -> None:
    """Should handle None result from generate_func"""

    async def generate_func() -> Optional[TJSONExtChunkStream]:
        return None

    situation, result = await cache_helper.run_with_cache(
        generate_func=generate_func,
        cache_options=cache_options,
    )

    assert situation in (CacheSituation.cache_disabled, CacheSituation.generated)
    assert result is None


@pytest.mark.asyncio
async def test_run_with_cache_locked(
    cache_helper: CacheProcessingHelper,
    cache_options: BIQueryCacheOptions,
) -> None:
    """Should handle locked cache case"""

    async def generate_func() -> Optional[TJSONExtChunkStream]:
        return AsyncChunked.from_chunked_iterable([[1, 2, 3]])

    situation, result = await cache_helper.run_with_cache(
        generate_func=generate_func,
        cache_options=cache_options,
        use_locked_cache=True,
    )

    assert situation in (CacheSituation.cache_disabled, CacheSituation.generated)
    assert result is not None
    data = await result.all()
    assert data == [1, 2, 3]


@pytest.mark.asyncio
async def test_run_with_cache_no_read(
    cache_helper: CacheProcessingHelper,
    cache_options: BIQueryCacheOptions,
) -> None:
    """Should handle case when cache read is not allowed"""

    async def generate_func() -> Optional[TJSONExtChunkStream]:
        return AsyncChunked.from_chunked_iterable([[1, 2, 3]])

    situation, result = await cache_helper.run_with_cache(
        generate_func=generate_func,
        cache_options=cache_options,
        allow_cache_read=False,
    )

    assert situation in (CacheSituation.cache_disabled, CacheSituation.generated)
    assert result is not None
    data = await result.all()
    assert data == [1, 2, 3]
