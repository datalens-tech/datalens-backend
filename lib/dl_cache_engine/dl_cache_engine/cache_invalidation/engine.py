import logging
import time
from typing import (
    Awaitable,
    Callable,
)
import uuid

import attr
from redis.asyncio import Redis

from dl_app_tools.profiling_base import generic_profiler_async
from dl_cache_engine.cache_invalidation.exc import CacheInvalidationDeserializationError
from dl_cache_engine.cache_invalidation.primitives import (
    CacheInvalidationEntry,
    CacheInvalidationKey,
)
from dl_cache_engine.cache_invalidation.schemas import deserialize_cache_invalidation_entry


LOGGER = logging.getLogger(__name__)

RESOURCE_TAG = "bic_cache_inval"

DATA_TTL_SEC = 86400  # 24h
LOCK_TTL_SEC = 30


TCacheInvalidationGenerateFunc = Callable[[], Awaitable[CacheInvalidationEntry]]


@attr.s(auto_attribs=True)
class StaleWhileRevalidateRedis:
    _redis_client: Redis = attr.ib(repr=False)
    _resource_tag: str = attr.ib(default=RESOURCE_TAG)
    _data_ttl_sec: int = attr.ib(default=DATA_TTL_SEC)
    _lock_ttl_sec: int = attr.ib(default=LOCK_TTL_SEC)

    def _make_data_key(self, key: CacheInvalidationKey) -> str:
        return f"{self._resource_tag}/data:{key.to_redis_key()}"

    def _make_lock_key(self, key: CacheInvalidationKey) -> str:
        return f"{self._resource_tag}/lock:{key.to_redis_key()}"

    @generic_profiler_async("inval-cache-swr-get-data")  # type: ignore  # TODO: fix
    async def get_data(self, key: CacheInvalidationKey) -> CacheInvalidationEntry | None:
        data_key = self._make_data_key(key)
        raw: bytes | None = await self._redis_client.get(data_key)
        if raw is None:
            return None

        try:
            return deserialize_cache_invalidation_entry(raw)
        except CacheInvalidationDeserializationError:
            LOGGER.warning(
                "Corrupted invalidation cache entry key=%s",
                data_key,
                exc_info=True,
            )
            return None

    @generic_profiler_async("inval-cache-swr-save-data")  # type: ignore  # TODO: fix
    async def save_data(self, key: CacheInvalidationKey, entry: CacheInvalidationEntry) -> None:
        data_key = self._make_data_key(key)
        data = entry.to_json_bytes()
        await self._redis_client.set(data_key, data, ex=self._data_ttl_sec)

    @generic_profiler_async("inval-cache-swr-try-acquire-lock")  # type: ignore  # TODO: fix
    async def try_acquire_lock(self, key: CacheInvalidationKey) -> bool:
        lock_key = self._make_lock_key(key)
        lock_value = uuid.uuid4().hex
        result = await self._redis_client.set(lock_key, lock_value, nx=True, ex=self._lock_ttl_sec)
        return result is not None

    @generic_profiler_async("inval-cache-swr-release-lock")  # type: ignore  # TODO: fix
    async def release_lock(self, key: CacheInvalidationKey) -> None:
        lock_key = self._make_lock_key(key)
        await self._redis_client.delete(lock_key)


@attr.s(auto_attribs=True)
class CacheInvalidationEngine:
    _swr_redis: StaleWhileRevalidateRedis = attr.ib()

    @staticmethod
    def _is_stale(entry: CacheInvalidationEntry, throttling_interval_sec: float) -> bool:
        return time.time() - entry.executed_at >= throttling_interval_sec

    @generic_profiler_async("inval-cache-get-stale-and-generate")  # type: ignore  # TODO: fix
    async def get_stale_and_generate(
        self,
        key: CacheInvalidationKey,
        throttling_interval_sec: float,
        generate_func: TCacheInvalidationGenerateFunc,
    ) -> str | None:
        redis = self._swr_redis
        redis_key_str = key.to_redis_key()

        existing_entry: CacheInvalidationEntry | None = None
        try:
            existing_entry = await redis.get_data(key)
        except Exception:
            LOGGER.exception(
                "Error reading invalidation cache key=%s",
                redis_key_str,
            )

        if existing_entry is not None and not self._is_stale(existing_entry, throttling_interval_sec):
            return existing_entry.data

        stale_data: str | None = existing_entry.data if existing_entry is not None else None

        lock_acquired = False
        try:
            lock_acquired = await redis.try_acquire_lock(key)
        except Exception:
            LOGGER.exception(
                "Error acquiring invalidation cache lock key=%s",
                redis_key_str,
            )

        if not lock_acquired:
            return stale_data

        try:
            new_entry = await generate_func()
            await redis.save_data(key, new_entry)

            if not new_entry.is_success:
                LOGGER.warning(
                    "Invalidation query returned error for key=%s: %s",
                    redis_key_str,
                    new_entry.payload,
                )
        except Exception:
            LOGGER.exception(
                "Error during invalidation cache revalidation key=%s",
                redis_key_str,
            )
        finally:
            try:
                await redis.release_lock(key)
            except Exception:
                LOGGER.exception(
                    "Error releasing invalidation cache lock key=%s",
                    redis_key_str,
                )

        return stale_data
