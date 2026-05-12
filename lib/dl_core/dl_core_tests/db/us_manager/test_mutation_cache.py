from __future__ import annotations

import asyncio
import datetime
from typing import (
    Any,
    AsyncGenerator,
)
import uuid

import attr
import pytest
import pytest_asyncio
from redis.asyncio import Redis

from dl_core.fields import (
    BIField,
    FormulaCalculationSpec,
    ParameterCalculationSpec,
)
from dl_core.us_dataset import Dataset
from dl_core.us_manager.mutation_cache.mutation_key_base import MutationKey
from dl_core.us_manager.mutation_cache.usentry_mutation_cache import (
    MemoryCacheEngine,
    RedisCacheEngine,
    USEntryMutationCache,
)
from dl_core.us_manager.us_manager_sync import SyncUSManager
from dl_core_tests.db.base import DefaultCoreTestClass
from dl_model_tools.typed_values import DateValue


@attr.s(auto_attribs=True, kw_only=True, frozen=True)
class MutationKeyForTest(MutationKey):
    _value: str

    def get_collision_tier_breaker(self) -> Any:
        return self._value

    def get_hash(self) -> str:
        return self._value


class TestMutationCache(DefaultCoreTestClass):
    @pytest_asyncio.fixture(scope="function")
    async def mutation_async_redis(self) -> AsyncGenerator[Redis, None]:
        redis_settings = self.core_test_config.get_redis_setting_maker().get_redis_settings_mutation()
        client: Redis = Redis(
            host=redis_settings.HOSTS[0],
            port=redis_settings.PORT,
            db=redis_settings.DB,
            password=redis_settings.PASSWORD,
        )
        try:
            yield client
        finally:
            await client.close()
            await client.connection_pool.disconnect()

    @pytest.fixture
    def redis_cache_engine(self, mutation_async_redis: Redis) -> RedisCacheEngine:
        return RedisCacheEngine(mutation_async_redis)

    @pytest.fixture
    def inmemory_cache_engine(self) -> MemoryCacheEngine:
        return MemoryCacheEngine()

    @pytest.fixture(params=["redis_cache_engine", "inmemory_cache_engine"])
    def cache_engine(self, request: pytest.FixtureRequest) -> Any:
        return request.getfixturevalue(request.param)

    @pytest.mark.asyncio
    async def test_mutation_cache(
        self,
        saved_dataset: Dataset,
        sync_us_manager: SyncUSManager,
        cache_engine: Any,
    ) -> None:
        # Update DS
        ds = saved_dataset
        field = BIField.make(
            guid=str(uuid.uuid4()),
            title="Test field",
            calc_spec=FormulaCalculationSpec(formula="1", guid_formula="1"),
        )
        ds.result_schema.add(field, idx=0)
        field = BIField.make(
            guid=str(uuid.uuid4()),
            title="Test field",
            calc_spec=ParameterCalculationSpec(default_value=DateValue(value=datetime.date(2022, 4, 25))),
        )
        ds.result_schema.add(field, idx=1)

        # Create Cache and mutation key
        cache = USEntryMutationCache(usm=sync_us_manager, cache_engine=cache_engine, default_ttl=1)
        mutation_key = MutationKeyForTest(value="test_hash")
        # `save_mutation_cache` substitutes a None `revision_id` with "" to build the cache key;
        # production callers (see `dl_api_lib.../dataset/base.py`) apply the same substitution
        # before lookup, so mirror it here.
        get_from_cache_params = (Dataset, ds.uuid, ds.revision_id or "", mutation_key)

        # Test no data in cache
        assert (await cache.get_mutated_entry_from_cache(*get_from_cache_params)) is None

        # Save and load from cache
        await cache.save_mutation_cache(ds, mutation_key)
        cached_data = await cache.get_mutated_entry_from_cache(*get_from_cache_params)

        # Compare original and cached dataset
        assert cached_data is not None
        assert isinstance(cached_data, Dataset)
        assert cached_data.result_schema == ds.result_schema
        # assert cached_data.data == ds.data  FIXME: BI-3257

        # Wait for expired TTL and check there is no more data
        await asyncio.sleep(2)
        expired_cached_data = await cache.get_mutated_entry_from_cache(*get_from_cache_params)
        assert expired_cached_data is None
