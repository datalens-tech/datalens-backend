import abc
import datetime
import json
import logging
import time
from typing import (
    Dict,
    Optional,
    Tuple,
    Type,
)

import attr
from redis.asyncio import Redis
import redis.exceptions

from dl_core.us_entry import USEntry
from dl_core.us_manager.mutation_cache.mutation_key_base import MutationKey
from dl_core.us_manager.us_manager import USManagerBase


LOGGER = logging.getLogger(__name__)


class GenericCacheEngine(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def save(self, key: str, data: str, ttl: float) -> None:
        raise NotImplementedError()

    @abc.abstractmethod
    async def load(self, key: str) -> Optional[str]:
        raise NotImplementedError()


class MemoryCacheEngine(GenericCacheEngine):
    def __init__(self) -> None:
        # Dict[ Key: (expired_time, cached value) ]
        self._cache: Dict[str, Tuple[float, str]] = dict()

    async def save(self, key: str, data: str, ttl: float) -> None:
        self._cache[key] = (time.monotonic() + ttl, data)

    async def load(self, key: str) -> Optional[str]:
        cached = self._cache.get(key)
        if not cached:
            return None
        expires_in, cached_value = cached
        if expires_in < time.monotonic():
            return None
        return cached_value


class MutationCacheError(redis.exceptions.ConnectionError):
    pass


@attr.s
class RedisCacheEngine(GenericCacheEngine):
    redis: Redis = attr.ib()

    async def save(self, key: str, data: str, ttl: float) -> None:
        try:
            await self.redis.set(name=key, value=data, ex=datetime.timedelta(seconds=ttl))
        except redis.exceptions.ConnectionError as e:
            LOGGER.warning("Error while saving to redis cache: connection error")
            raise MutationCacheError() from e

    async def load(self, key: str) -> Optional[str]:
        try:
            return await self.redis.get(key)
        except redis.exceptions.ConnectionError as e:
            LOGGER.warning("Error while loading from redis cache: connection error")
            raise MutationCacheError() from e


@attr.s(auto_attribs=True, frozen=True, kw_only=True)
class USEntryMutationCacheKey:
    scope: str
    entry_id: str
    entry_revision_id: str
    mutation_key_hash: str

    def to_string(self) -> str:
        return f"{self.scope}:{self.entry_id}:{self.entry_revision_id}:{self.mutation_key_hash}"


@attr.s()
class USEntryMutationCache:
    _usm: USManagerBase = attr.ib()
    _cache_engine: GenericCacheEngine = attr.ib()
    _default_ttl: float = attr.ib()

    def _prepare_cache_data(self, entry: USEntry, mutation_key: MutationKey) -> str:
        serialized_entry = self._usm._get_entry_save_params(entry)
        serialized_entry["key"] = entry.raw_us_key
        serialized_entry["entryId"] = entry.uuid
        serialized_entry["unversionedData"] = serialized_entry.pop("unversioned_data")
        return self._dump_raw_cache_data(mutation_key.get_collision_tier_breaker(), serialized_entry)

    async def _restore_entry_from_cache_representation(self, data: str) -> USEntry:
        entry_dict = json.loads(data)
        obj = self._usm._entry_dict_to_obj(entry_dict)
        await self._usm.get_lifecycle_manager(entry=obj).post_init_async_hook()
        return obj

    async def save_mutation_cache(self, entry: USEntry, mutation_key: MutationKey) -> None:
        key = USEntryMutationCacheKey(
            scope=entry.scope,  # type: ignore  # TODO: Fix
            entry_id=entry.uuid,  # type: ignore  # TODO: Fix
            entry_revision_id=entry.revision_id,
            mutation_key_hash=mutation_key.get_hash(),
        )
        data = self._prepare_cache_data(entry, mutation_key)
        try:
            await self._cache_engine.save(key=key.to_string(), data=data, ttl=self._default_ttl)
        except MutationCacheError:
            LOGGER.warning("Error saving to cache", exc_info=True)

    @classmethod
    def _load_raw_cache_data(cls, data: str) -> Tuple[str, str]:
        all_data: dict = json.loads(data)
        return (
            all_data["collision_meta"],
            all_data["us_entry_data"],
        )

    @classmethod
    def _dump_raw_cache_data(cls, collision_meta: str, us_entry_data: dict) -> str:
        return json.dumps(
            {"us_entry_data": json.dumps(us_entry_data), "collision_meta": collision_meta}, ensure_ascii=True
        )

    # TODO FIX: Consider passing whole USEntry to decrease possibility of unauthorized access to cache
    async def get_mutated_entry_from_cache(
        self,
        expected_type: Type[USEntry],
        entry_id: str,
        revision_id: str,
        mutation_key: MutationKey,
    ) -> Optional[USEntry]:
        key = USEntryMutationCacheKey(
            scope=expected_type.scope,  # type: ignore  # TODO: Fix
            entry_id=entry_id,
            entry_revision_id=revision_id,
            mutation_key_hash=mutation_key.get_hash(),
        )

        try:
            data = await self._cache_engine.load(key.to_string())
        except MutationCacheError:
            LOGGER.warning("Error loading from cache", exc_info=True)
            return None

        if data is None:
            return None

        collision_meta, us_entry_data = self._load_raw_cache_data(data)
        if collision_meta != mutation_key.get_collision_tier_breaker():
            return None

        return await self._restore_entry_from_cache_representation(us_entry_data)
