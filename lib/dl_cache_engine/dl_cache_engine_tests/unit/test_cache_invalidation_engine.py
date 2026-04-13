import asyncio
import time
from unittest.mock import AsyncMock

import pytest

from dl_cache_engine.cache_invalidation.engine import (
    RESOURCE_TAG,
    CacheInvalidationEngine,
    StaleWhileRevalidateRedis,
)
from dl_cache_engine.cache_invalidation.primitives import (
    CacheInvalidationEntry,
    CacheInvalidationErrorPayload,
    CacheInvalidationKey,
    CacheInvalidationStatus,
    CacheInvalidationSuccessPayload,
)
from dl_cache_engine.cache_invalidation.schemas import deserialize_cache_invalidation_entry


# ===== Helpers =====


def _make_key() -> CacheInvalidationKey:
    return CacheInvalidationKey(
        dataset_id="ds-1",
        dataset_revision_id="rev-1",
        connection_id="conn-1",
        connection_revision_id="conn-rev-1",
    )


def _make_success_entry(data: str = "test_payload", executed_at: float | None = None) -> CacheInvalidationEntry:
    return CacheInvalidationEntry(
        status=CacheInvalidationStatus.SUCCESS,
        payload=CacheInvalidationSuccessPayload(data=data),
        executed_at=executed_at if executed_at is not None else time.time(),
    )


def _make_error_entry(executed_at: float | None = None) -> CacheInvalidationEntry:
    return CacheInvalidationEntry(
        status=CacheInvalidationStatus.ERROR,
        payload=CacheInvalidationErrorPayload(
            error_code="ERR.DS.TEST",
            error_message="Test error",
        ),
        executed_at=executed_at if executed_at is not None else time.time(),
    )


def _make_mock_redis() -> AsyncMock:
    """Create a mock Redis client with async methods."""
    mock = AsyncMock()
    mock.get = AsyncMock(return_value=None)
    mock.set = AsyncMock(return_value=True)
    mock.delete = AsyncMock(return_value=1)
    return mock


def _make_swr_mock() -> AsyncMock:
    """Create a mock StaleWhileRevalidateRedis."""
    mock = AsyncMock(spec=StaleWhileRevalidateRedis)
    mock.get_data = AsyncMock(return_value=None)
    mock.save_data = AsyncMock()
    mock.try_acquire_lock = AsyncMock(return_value=False)
    mock.release_lock = AsyncMock()
    return mock


# ===== StaleWhileRevalidateRedis key tests =====


def test_swr_data_key_format() -> None:
    swr = StaleWhileRevalidateRedis(redis_client=_make_mock_redis())
    key = _make_key()
    data_key = swr._make_data_key(key)
    assert data_key.startswith(f"{RESOURCE_TAG}/data:cache_inval_")


def test_swr_lock_key_format() -> None:
    swr = StaleWhileRevalidateRedis(redis_client=_make_mock_redis())
    key = _make_key()
    lock_key = swr._make_lock_key(key)
    assert lock_key.startswith(f"{RESOURCE_TAG}/lock:cache_inval_")


def test_swr_data_and_lock_keys_share_suffix() -> None:
    swr = StaleWhileRevalidateRedis(redis_client=_make_mock_redis())
    key = _make_key()
    data_suffix = swr._make_data_key(key).split(":")[-1]
    lock_suffix = swr._make_lock_key(key).split(":")[-1]
    assert data_suffix == lock_suffix


# ===== StaleWhileRevalidateRedis get_data tests =====


@pytest.mark.asyncio
async def test_swr_get_data_returns_none_when_key_missing() -> None:
    mock_redis = _make_mock_redis()
    mock_redis.get.return_value = None
    swr = StaleWhileRevalidateRedis(redis_client=mock_redis)
    assert await swr.get_data(_make_key()) is None


@pytest.mark.asyncio
async def test_swr_get_data_returns_entry_when_data_exists() -> None:
    entry = _make_success_entry(data="cached_value")
    mock_redis = _make_mock_redis()
    mock_redis.get.return_value = entry.to_json_bytes()
    swr = StaleWhileRevalidateRedis(redis_client=mock_redis)

    result = await swr.get_data(_make_key())
    assert result is not None
    assert result.data == "cached_value"


@pytest.mark.asyncio
async def test_swr_get_data_returns_none_on_corrupted_data() -> None:
    mock_redis = _make_mock_redis()
    mock_redis.get.return_value = b"not valid json"
    swr = StaleWhileRevalidateRedis(redis_client=mock_redis)
    assert await swr.get_data(_make_key()) is None


# ===== StaleWhileRevalidateRedis save_data tests =====


@pytest.mark.asyncio
async def test_swr_saves_serialized_entry() -> None:
    mock_redis = _make_mock_redis()
    swr = StaleWhileRevalidateRedis(redis_client=mock_redis)
    entry = _make_success_entry(data="save_test")

    await swr.save_data(_make_key(), entry)

    saved_data = mock_redis.set.call_args[0][1]
    restored = deserialize_cache_invalidation_entry(saved_data)
    assert restored.data == "save_test"


# ===== StaleWhileRevalidateRedis lock tests =====


@pytest.mark.asyncio
async def test_swr_acquire_lock_success() -> None:
    mock_redis = _make_mock_redis()
    mock_redis.set.return_value = True
    swr = StaleWhileRevalidateRedis(redis_client=mock_redis)
    assert await swr.try_acquire_lock(_make_key()) is True


@pytest.mark.asyncio
async def test_swr_acquire_lock_failure() -> None:
    mock_redis = _make_mock_redis()
    mock_redis.set.return_value = None
    swr = StaleWhileRevalidateRedis(redis_client=mock_redis)
    assert await swr.try_acquire_lock(_make_key()) is False


@pytest.mark.asyncio
async def test_swr_release_lock() -> None:
    mock_redis = _make_mock_redis()
    swr = StaleWhileRevalidateRedis(redis_client=mock_redis)
    await swr.release_lock(_make_key())
    mock_redis.delete.assert_called_once()


# ===== CacheInvalidationEngine._is_stale tests =====


def test_engine_fresh_entry() -> None:
    entry = _make_success_entry(executed_at=time.time())
    assert CacheInvalidationEngine._is_stale(entry, 60.0) is False


def test_engine_stale_entry() -> None:
    entry = _make_success_entry(executed_at=time.time() - 120)
    assert CacheInvalidationEngine._is_stale(entry, 60.0) is True


def test_engine_exactly_at_boundary() -> None:
    entry = _make_success_entry(executed_at=time.time() - 60)
    assert CacheInvalidationEngine._is_stale(entry, 60.0) is True


# ===== CacheInvalidationEngine.get_stale_and_generate tests =====


@pytest.mark.asyncio
async def test_engine_fresh_data_returned_immediately() -> None:
    swr_mock = _make_swr_mock()
    swr_mock.get_data.return_value = _make_success_entry(data="fresh", executed_at=time.time())

    engine = CacheInvalidationEngine(swr_redis=swr_mock)
    generate_func = AsyncMock()

    result = await engine.get_stale_and_generate(_make_key(), 60.0, generate_func)

    assert result == "fresh"
    swr_mock.try_acquire_lock.assert_not_called()
    generate_func.assert_not_called()


@pytest.mark.asyncio
async def test_engine_stale_data_lock_not_acquired() -> None:
    swr_mock = _make_swr_mock()
    swr_mock.get_data.return_value = _make_success_entry(data="stale", executed_at=time.time() - 120)
    swr_mock.try_acquire_lock.return_value = False

    engine = CacheInvalidationEngine(swr_redis=swr_mock)
    generate_func = AsyncMock()

    result = await engine.get_stale_and_generate(_make_key(), 60.0, generate_func)

    assert result == "stale"
    generate_func.assert_not_called()


@pytest.mark.asyncio
async def test_engine_stale_data_lock_acquired_returns_stale() -> None:
    swr_mock = _make_swr_mock()
    swr_mock.get_data.return_value = _make_success_entry(data="stale", executed_at=time.time() - 120)
    swr_mock.try_acquire_lock.return_value = True

    new_entry = _make_success_entry(data="new")
    generate_func = AsyncMock(return_value=new_entry)

    engine = CacheInvalidationEngine(swr_redis=swr_mock)
    result = await engine.get_stale_and_generate(_make_key(), 60.0, generate_func)

    assert result == "stale"  # NOT "new"
    generate_func.assert_called_once()
    swr_mock.save_data.assert_called_once()
    swr_mock.release_lock.assert_called_once()


@pytest.mark.asyncio
async def test_engine_empty_cache_returns_none() -> None:
    swr_mock = _make_swr_mock()
    swr_mock.get_data.return_value = None
    swr_mock.try_acquire_lock.return_value = True

    new_entry = _make_success_entry(data="first")
    generate_func = AsyncMock(return_value=new_entry)

    engine = CacheInvalidationEngine(swr_redis=swr_mock)
    result = await engine.get_stale_and_generate(_make_key(), 60.0, generate_func)

    assert result is None
    generate_func.assert_called_once()
    swr_mock.save_data.assert_called_once()


@pytest.mark.asyncio
async def test_engine_empty_cache_lock_not_acquired() -> None:
    swr_mock = _make_swr_mock()
    swr_mock.get_data.return_value = None
    swr_mock.try_acquire_lock.return_value = False

    engine = CacheInvalidationEngine(swr_redis=swr_mock)
    generate_func = AsyncMock()

    result = await engine.get_stale_and_generate(_make_key(), 60.0, generate_func)

    assert result is None
    generate_func.assert_not_called()


@pytest.mark.asyncio
async def test_engine_error_entry_stale_data_is_none() -> None:
    swr_mock = _make_swr_mock()
    swr_mock.get_data.return_value = _make_error_entry(executed_at=time.time() - 120)
    swr_mock.try_acquire_lock.return_value = True

    generate_func = AsyncMock(return_value=_make_success_entry(data="recovered"))

    engine = CacheInvalidationEngine(swr_redis=swr_mock)
    result = await engine.get_stale_and_generate(_make_key(), 60.0, generate_func)

    assert result is None
    generate_func.assert_called_once()


@pytest.mark.asyncio
async def test_engine_generate_func_error_releases_lock() -> None:
    swr_mock = _make_swr_mock()
    swr_mock.get_data.return_value = None
    swr_mock.try_acquire_lock.return_value = True

    generate_func = AsyncMock(side_effect=RuntimeError("query failed"))

    engine = CacheInvalidationEngine(swr_redis=swr_mock)
    result = await engine.get_stale_and_generate(_make_key(), 60.0, generate_func)

    assert result is None
    swr_mock.release_lock.assert_called_once()
    swr_mock.save_data.assert_not_called()


@pytest.mark.asyncio
async def test_engine_error_entry_saved_to_redis() -> None:
    swr_mock = _make_swr_mock()
    swr_mock.get_data.return_value = _make_success_entry(data="old", executed_at=time.time() - 120)
    swr_mock.try_acquire_lock.return_value = True

    error_entry = _make_error_entry()
    generate_func = AsyncMock(return_value=error_entry)

    engine = CacheInvalidationEngine(swr_redis=swr_mock)
    result = await engine.get_stale_and_generate(_make_key(), 60.0, generate_func)

    assert result == "old"
    saved_entry = swr_mock.save_data.call_args[0][1]
    assert not saved_entry.is_success


@pytest.mark.asyncio
async def test_engine_redis_get_error_graceful() -> None:
    swr_mock = _make_swr_mock()
    swr_mock.get_data.side_effect = ConnectionError("Redis down")
    swr_mock.try_acquire_lock.return_value = True

    generate_func = AsyncMock(return_value=_make_success_entry(data="new"))

    engine = CacheInvalidationEngine(swr_redis=swr_mock)
    result = await engine.get_stale_and_generate(_make_key(), 60.0, generate_func)

    assert result is None
    generate_func.assert_called_once()


@pytest.mark.asyncio
async def test_engine_redis_lock_error_graceful() -> None:
    swr_mock = _make_swr_mock()
    swr_mock.get_data.return_value = _make_success_entry(data="stale", executed_at=time.time() - 120)
    swr_mock.try_acquire_lock.side_effect = ConnectionError("Redis down")

    generate_func = AsyncMock()

    engine = CacheInvalidationEngine(swr_redis=swr_mock)
    result = await engine.get_stale_and_generate(_make_key(), 60.0, generate_func)

    assert result == "stale"
    generate_func.assert_not_called()


# ===== CacheInvalidationEngine concurrency tests =====
#
# Test the core SWR property: when multiple requests arrive concurrently
# with stale data, only one acquires the lock and generates, while the
# rest return stale data immediately.


@pytest.mark.asyncio
async def test_engine_concurrent_requests_only_one_generates() -> None:
    stale_entry = _make_success_entry(data="stale_value", executed_at=time.time() - 120)
    new_entry = _make_success_entry(data="new_value")

    generate_call_count = 0
    generate_event = asyncio.Event()

    async def slow_generate_func() -> CacheInvalidationEntry:
        nonlocal generate_call_count
        generate_call_count += 1
        # Simulate slow query — other requests should not wait
        await asyncio.sleep(0.1)
        generate_event.set()
        return new_entry

    # Mock SWR Redis with realistic lock behavior:
    # First call to try_acquire_lock returns True, rest return False
    lock_acquired = False

    swr_mock = _make_swr_mock()
    swr_mock.get_data.return_value = stale_entry

    async def mock_try_acquire_lock(key: CacheInvalidationKey) -> bool:
        nonlocal lock_acquired
        if not lock_acquired:
            lock_acquired = True
            return True
        return False

    swr_mock.try_acquire_lock.side_effect = mock_try_acquire_lock

    engine = CacheInvalidationEngine(swr_redis=swr_mock)
    key = _make_key()

    # Launch 5 concurrent requests
    results = await asyncio.gather(*[engine.get_stale_and_generate(key, 60.0, slow_generate_func) for _ in range(5)])

    # All should return stale data
    assert all(r == "stale_value" for r in results)
    # generate_func should be called exactly once
    assert generate_call_count == 1
    # Data should be saved exactly once
    assert swr_mock.save_data.call_count == 1
    # Lock should be released exactly once
    assert swr_mock.release_lock.call_count == 1


@pytest.mark.asyncio
async def test_engine_concurrent_requests_empty_cache_all_return_none() -> None:
    new_entry = _make_success_entry(data="first_value")
    generate_call_count = 0

    async def generate_func() -> CacheInvalidationEntry:
        nonlocal generate_call_count
        generate_call_count += 1
        await asyncio.sleep(0.05)
        return new_entry

    lock_acquired = False

    swr_mock = _make_swr_mock()
    swr_mock.get_data.return_value = None  # empty cache

    async def mock_try_acquire_lock(key: CacheInvalidationKey) -> bool:
        nonlocal lock_acquired
        if not lock_acquired:
            lock_acquired = True
            return True
        return False

    swr_mock.try_acquire_lock.side_effect = mock_try_acquire_lock

    engine = CacheInvalidationEngine(swr_redis=swr_mock)

    results = await asyncio.gather(*[engine.get_stale_and_generate(_make_key(), 60.0, generate_func) for _ in range(5)])

    # All return None (empty cache)
    assert all(r is None for r in results)
    # generate_func called exactly once
    assert generate_call_count == 1
