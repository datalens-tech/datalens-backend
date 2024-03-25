import re

import pytest
import pytest_asyncio
import redis.asyncio

import dl_rate_limiter


@pytest.fixture(name="request_patterns")
def fixture_request_patterns() -> list[dl_rate_limiter.RequestPattern]:
    return [
        dl_rate_limiter.RequestPattern(
            url_regex=re.compile(r"/limited/more_specifically.*"),
            methods=frozenset(["GET"]),
            event_key_template=dl_rate_limiter.RequestEventKeyTemplate(
                key="more_specifically_limited", headers=frozenset(["X-Test-Header"])
            ),
            limit=1,
            window_ms=1000,
        ),
        dl_rate_limiter.RequestPattern(
            url_regex=re.compile(r"/limited/.*"),
            methods=frozenset(["GET"]),
            event_key_template=dl_rate_limiter.RequestEventKeyTemplate(
                key="limited", headers=frozenset(["X-Test-Header"])
            ),
            limit=5,
            window_ms=1000,
        ),
    ]


@pytest.fixture(name="sync_request_limiter", scope="function")
def fixture_sync_request_limiter(
    sync_redis_client: redis.Redis,
    request_patterns: list[dl_rate_limiter.RequestPattern],
) -> dl_rate_limiter.SyncRequestRateLimiter:
    event_limiter = dl_rate_limiter.SyncRedisEventRateLimiter(sync_redis_client)
    event_limiter.prepare()

    return dl_rate_limiter.SyncRequestRateLimiter(
        event_limiter=event_limiter,
        patterns=request_patterns,
    )


@pytest_asyncio.fixture(name="async_request_limiter", scope="function")
async def fixture_async_request_limiter(
    async_redis_client: redis.asyncio.Redis,
    request_patterns: list[dl_rate_limiter.RequestPattern],
) -> dl_rate_limiter.AsyncRequestRateLimiter:
    event_limiter = dl_rate_limiter.AsyncRedisEventRateLimiter(async_redis_client)
    await event_limiter.prepare()

    return dl_rate_limiter.AsyncRequestRateLimiter(
        event_limiter=event_limiter,
        patterns=request_patterns,
    )
