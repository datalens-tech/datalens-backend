import asyncio

import pytest
import pytest_asyncio
import redis
import redis.asyncio

from dl_testing.containers import (
    HostPort,
    get_test_container_hostport,
)
from dl_testing.utils import wait_for_port


@pytest.fixture(scope="session")
def event_loop():
    """Avoid spontaneous event loop closes between tests"""
    loop = asyncio.get_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="session", name="redis_hostport")
def fixture_redis_hostport() -> HostPort:
    redis_hostport = get_test_container_hostport("redis", fallback_port=51612)
    wait_for_port(redis_hostport.host, redis_hostport.port)

    return redis_hostport


@pytest.fixture(scope="session", name="session_sync_redis_client")
def fixture_session_sync_redis_client(redis_hostport: HostPort) -> redis.Redis:
    return redis.Redis(host=redis_hostport.host, port=redis_hostport.port)


@pytest.fixture(scope="function", name="sync_redis_client")
def fixture_sync_redis_client(session_sync_redis_client: redis.Redis) -> redis.Redis:
    session_sync_redis_client.flushall()
    session_sync_redis_client.function_flush()

    return session_sync_redis_client


@pytest_asyncio.fixture(scope="session", name="session_async_redis_client")
async def fixture_session_async_redis_client(redis_hostport: HostPort) -> redis.asyncio.Redis:
    return redis.asyncio.Redis(host=redis_hostport.host, port=redis_hostport.port)


@pytest_asyncio.fixture(scope="function", name="async_redis_client")
async def fixture_async_redis_client(session_async_redis_client: redis.asyncio.Redis) -> redis.asyncio.Redis:
    await session_async_redis_client.flushall()
    await session_async_redis_client.function_flush()

    return session_async_redis_client
