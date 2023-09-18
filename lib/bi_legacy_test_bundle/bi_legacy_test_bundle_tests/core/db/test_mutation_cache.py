import asyncio
import datetime
from typing import Any
import uuid

import attr
import pytest

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
from dl_core.values import DateValue


@attr.s(auto_attribs=True, kw_only=True, frozen=True)
class MutationKeyForTest(MutationKey):
    _value: str

    def get_collision_tier_breaker(self) -> Any:
        return self._value

    def get_hash(self) -> str:
        return self._value


@pytest.fixture
def redis_cache_engine(mutation_async_redis):
    return RedisCacheEngine(mutation_async_redis)


@pytest.fixture
def inmemory_cache_engine():
    return MemoryCacheEngine()


@pytest.fixture(params=["redis_cache_engine", "inmemory_cache_engine"])
def cache_engine(request):
    return request.getfixturevalue(request.param)


async def test_mutation_cache(saved_dataset, default_sync_usm, cache_engine):
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
    cache = USEntryMutationCache(usm=default_sync_usm, cache_engine=cache_engine, default_ttl=1)
    mutation_key = MutationKeyForTest(value="test_hash")
    get_from_cache_params = (Dataset, ds.uuid, ds.revision_id, mutation_key)

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
