import asyncio
from multiprocessing.pool import ThreadPool
import time

import flaky
import pytest
import redis.asyncio

import dl_rate_limiter


class Timer:
    def __init__(self):
        self._start = time.time()

    def get_time_left(self, seconds: float) -> float:
        time_left = seconds - (time.time() - self._start)
        return max(time_left, 0)


class SyncTimer(Timer):
    def sleep_from_start(self, seconds: float):
        time.sleep(self.get_time_left(seconds))


class AsyncTimer(Timer):
    async def sleep_from_start(self, seconds: float):
        await asyncio.sleep(self.get_time_left(seconds))


def test_sync_load_function(sync_redis_client: redis.Redis):
    functions = sync_redis_client.function_list()
    assert functions == []

    rate_limiter = dl_rate_limiter.SyncRedisEventRateLimiter(sync_redis_client)
    rate_limiter._load_function()

    functions = sync_redis_client.function_list()
    assert functions != []  # some functions are loaded


@pytest.mark.asyncio
async def test_async_load_function(async_redis_client: redis.asyncio.Redis):
    functions = await async_redis_client.function_list()
    assert functions == []

    rate_limiter = dl_rate_limiter.AsyncRedisEventRateLimiter(async_redis_client)
    await rate_limiter._load_function()

    functions = await async_redis_client.function_list()
    assert functions != []  # some functions are loaded


def test_sync_load_function_concurrent(sync_redis_client: redis.Redis):
    functions = sync_redis_client.function_list()
    assert functions == []

    rate_limiter = dl_rate_limiter.SyncRedisEventRateLimiter(sync_redis_client)
    with ThreadPool(100) as pool:
        pool.map(rate_limiter._load_function, range(100))

    functions = sync_redis_client.function_list()
    assert functions != []  # some functions are loaded


@pytest.mark.asyncio
async def test_async_load_function_concurrent(async_redis_client: redis.asyncio.Redis):
    functions = await async_redis_client.function_list()
    assert functions == []

    rate_limiter = dl_rate_limiter.AsyncRedisEventRateLimiter(async_redis_client)
    await asyncio.gather(*(rate_limiter._load_function() for _ in range(100)))

    functions = await async_redis_client.function_list()
    assert functions != []


def test_sync_check_event_limit_loads_function(sync_redis_client: redis.Redis):
    functions = sync_redis_client.function_list()
    assert functions == []

    rate_limiter = dl_rate_limiter.SyncRedisEventRateLimiter(sync_redis_client)
    result = rate_limiter.check_limit(event_key="key", limit=1, window_ms=1)
    assert result is True

    functions = sync_redis_client.function_list()
    assert functions != []


@pytest.mark.asyncio
async def test_async_check_event_limit_loads_function(async_redis_client: redis.asyncio.Redis):
    functions = await async_redis_client.function_list()
    assert functions == []

    rate_limiter = dl_rate_limiter.AsyncRedisEventRateLimiter(async_redis_client)
    result = await rate_limiter.check_limit(event_key="key", limit=1, window_ms=1)
    assert result is True

    functions = await async_redis_client.function_list()
    assert functions != []


def test_sync_check_event_limit(sync_redis_client: redis.Redis):
    rate_limiter = dl_rate_limiter.SyncRedisEventRateLimiter(sync_redis_client)
    rate_limiter.prepare()

    # checking 10 events with limit 5, expecting 5 to pass
    with ThreadPool(10) as pool:
        results = pool.map(lambda _: rate_limiter.check_limit(event_key="key", limit=5, window_ms=1000), range(10))
    assert sum(1 if result else 0 for result in results) == 5


@flaky.flaky(max_runs=3)
@pytest.mark.asyncio
async def test_async_check_event_limit(async_redis_client: redis.asyncio.Redis):
    rate_limiter = dl_rate_limiter.AsyncRedisEventRateLimiter(async_redis_client)
    await rate_limiter.prepare()

    # checking 10 events with limit 5, expecting 5 to pass
    tasks = [rate_limiter.check_limit(event_key="key", limit=5, window_ms=1000) for _ in range(10)]
    results = await asyncio.gather(*tasks)
    assert sum(1 if result else 0 for result in results) == 5


@flaky.flaky(max_runs=3)
def test_sync_check_event_limit_window_expiration(sync_redis_client: redis.Redis):
    rate_limiter = dl_rate_limiter.SyncRedisEventRateLimiter(sync_redis_client)
    rate_limiter.prepare()

    timer = SyncTimer()

    # checking 10 events with limit 5, expecting 5 to pass
    with ThreadPool(10) as pool:
        results = pool.map(lambda _: rate_limiter.check_limit(event_key="key", limit=5, window_ms=10), range(10))
    assert sum(1 if result else 0 for result in results) == 5

    # waiting for window to expire
    timer.sleep_from_start(0.02)

    # checking 10 events with limit 5, expecting 5 to pass
    with ThreadPool(10) as pool:
        results = pool.map(lambda _: rate_limiter.check_limit(event_key="key", limit=5, window_ms=10), range(10))
    assert sum(1 if result else 0 for result in results) == 5


@flaky.flaky(max_runs=3)
@pytest.mark.asyncio
async def test_async_check_event_limit_window_expiration(async_redis_client: redis.asyncio.Redis):
    rate_limiter = dl_rate_limiter.AsyncRedisEventRateLimiter(async_redis_client)
    await rate_limiter.prepare()

    timer = AsyncTimer()
    # checking 10 events with limit 5, expecting 5 to pass
    tasks = [rate_limiter.check_limit(event_key="key", limit=5, window_ms=10) for _ in range(10)]
    results = await asyncio.gather(*tasks)
    assert sum(1 if result else 0 for result in results) == 5

    # waiting for window to expire
    await timer.sleep_from_start(0.02)

    # checking 10 events with limit 5, expecting 5 to pass
    tasks = [rate_limiter.check_limit(event_key="key", limit=5, window_ms=10) for _ in range(10)]
    results = await asyncio.gather(*tasks)
    assert sum(1 if result else 0 for result in results) == 5


@flaky.flaky(max_runs=3)
def test_sync_check_event_limit_window_sliding(sync_redis_client: redis.Redis):
    rate_limiter = dl_rate_limiter.SyncRedisEventRateLimiter(sync_redis_client)
    rate_limiter.prepare()

    timer = SyncTimer()
    # checking 5 events with limit 10, expecting all to pass
    with ThreadPool(5) as pool:
        results = pool.map(lambda _: rate_limiter.check_limit(event_key="key", limit=10, window_ms=60), range(5))
    assert sum(1 if result else 0 for result in results) == 5

    # waiting for window to slide
    timer.sleep_from_start(0.035)

    # checking 10 events with limit 10, expecting 5 to pass
    with ThreadPool(10) as pool:
        results = pool.map(lambda _: rate_limiter.check_limit(event_key="key", limit=10, window_ms=60), range(10))
    assert sum(1 if result else 0 for result in results) == 5

    # waiting for window to slide
    timer.sleep_from_start(0.070)

    # checking 10 events with limit 10, expecting 5 to pass
    with ThreadPool(10) as pool:
        results = pool.map(lambda _: rate_limiter.check_limit(event_key="key", limit=10, window_ms=60), range(10))
    assert sum(1 if result else 0 for result in results) == 5


@flaky.flaky(max_runs=3)
@pytest.mark.asyncio
async def test_async_check_event_limit_window_sliding(async_redis_client: redis.asyncio.Redis):
    rate_limiter = dl_rate_limiter.AsyncRedisEventRateLimiter(async_redis_client)
    await rate_limiter.prepare()

    timer = AsyncTimer()
    # checking 5 events with limit 10, expecting all to pass
    tasks = [rate_limiter.check_limit(event_key="key", limit=10, window_ms=60) for _ in range(5)]
    results = await asyncio.gather(*tasks)
    assert sum(1 if result else 0 for result in results) == 5

    # waiting for window to slide
    await timer.sleep_from_start(0.035)

    # checking 10 events with limit 10, expecting 5 to pass
    tasks = [rate_limiter.check_limit(event_key="key", limit=10, window_ms=60) for _ in range(10)]
    results = await asyncio.gather(*tasks)
    assert sum(1 if result else 0 for result in results) == 5

    # waiting for window to slide
    await timer.sleep_from_start(0.070)

    # checking 10 events with limit 10, expecting 5 to pass
    tasks = [rate_limiter.check_limit(event_key="key", limit=10, window_ms=60) for _ in range(10)]
    results = await asyncio.gather(*tasks)
    assert sum(1 if result else 0 for result in results) == 5
