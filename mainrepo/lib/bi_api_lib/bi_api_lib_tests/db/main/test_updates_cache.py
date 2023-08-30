from __future__ import annotations

import redis.asyncio
import pytest

from bi_api_lib.api_common.update_dataset_mutation_key import UpdateDatasetMutationKey
from bi_api_lib.schemas.action import UpdateFieldActionSchema, AddFieldActionSchema
from bi_core.us_dataset import Dataset as UsDataset
from bi_core.us_manager.mutation_cache.usentry_mutation_cache import (
    RedisCacheEngine, USEntryMutationCache, USEntryMutationCacheKey
)
from bi_core.components.editor import DatasetComponentEditor
from bi_api_client.dsmaker.primitives import Dataset, DateParameterValue
from bi_api_client.dsmaker.shortcuts.dataset import add_parameters_to_dataset

field_index = 6  # Field index using for tests, contains datetime value


def get_updates(field_id):
    return [
        {
            'action': 'update_field',
            'field': {
                'guid': field_id,
                'cast': 'integer'
            },
            'order_index': 0
        }
    ]


@pytest.fixture
async def mutation_cache(redis_setting_maker, default_sync_usm_per_test):
    mutation_redis_settings = redis_setting_maker.get_redis_settings_mutation()
    mutation_async_redis = redis.asyncio.Redis(
        host=mutation_redis_settings.HOSTS[0],
        port=mutation_redis_settings.PORT,
        db=mutation_redis_settings.DB,
        password=mutation_redis_settings.PASSWORD,
    )
    yield USEntryMutationCache(default_sync_usm_per_test, RedisCacheEngine(mutation_async_redis), 60)
    await mutation_async_redis.close()
    await mutation_async_redis.connection_pool.disconnect()


def create_mutation_key(dataset: UsDataset, updates: list[dict]) -> UpdateDatasetMutationKey:
    schema_map = {
        'update_field': UpdateFieldActionSchema,
        'add_field': AddFieldActionSchema,
    }
    deserialized_updates = [schema_map[upd['action']]().load(upd) for upd in updates]
    return UpdateDatasetMutationKey.create(dataset.revision_id, deserialized_updates)


async def _get_from_cache(cache: USEntryMutationCache, dataset: UsDataset, key: UpdateDatasetMutationKey):
    return await cache.get_mutated_entry_from_cache(
        UsDataset,
        dataset.uuid,
        dataset.revision_id,
        key,
    )


async def _save_to_cache(
    cache: USEntryMutationCache,
    ds_for_key: UsDataset,
    ds_for_save: UsDataset,
    key: UpdateDatasetMutationKey
):
    assert isinstance(ds_for_key.uuid, str)
    cache_key = USEntryMutationCacheKey(
        scope=ds_for_key.scope,
        entry_id=ds_for_key.uuid,
        entry_revision_id=ds_for_key.revision_id,
        mutation_key_hash=key.get_hash(),
    )
    data = cache._prepare_cache_data(ds_for_save, key)  # noqa
    await cache._cache_engine.save(key=cache_key.to_string(), data=data, ttl=60)  # noqa


def test_save_to_cache(
    api_v1, data_api_v2_with_mutation_cache, mutation_cache, loop, dataset_id, default_sync_usm_per_test,
):
    data_api_v2 = data_api_v2_with_mutation_cache
    ds = api_v1.load_dataset(dataset=Dataset(id=dataset_id)).dataset
    ds = api_v1.save_dataset(dataset=ds).dataset
    ds_usentry = default_sync_usm_per_test.get_by_id(ds.id)
    updates = get_updates(ds.result_schema[field_index].id)
    field = ds.result_schema[field_index]

    key = create_mutation_key(dataset=ds_usentry, updates=updates)
    # Check no saved key in cache
    assert loop.run_until_complete(_get_from_cache(mutation_cache, dataset=ds_usentry, key=key)) is None
    # Make a request
    resp = data_api_v2.get_result(dataset=ds, updates=updates, fields=[field])
    assert resp.status_code == 200
    # Check now key is saved
    assert loop.run_until_complete(_get_from_cache(mutation_cache, dataset=ds_usentry, key=key)) is not None


def test_load_from_cache(
    api_v1, data_api_v2_with_mutation_cache, mutation_cache, loop, dataset_id, default_sync_usm_per_test,
):
    data_api_v2 = data_api_v2_with_mutation_cache
    ds = api_v1.load_dataset(dataset=Dataset(id=dataset_id)).dataset
    ds = api_v1.save_dataset(dataset=ds).dataset
    ds_usentry = default_sync_usm_per_test.get_by_id(ds.id)
    updates = get_updates(ds.result_schema[field_index].id)

    ds2 = api_v1.copy_dataset(ds, new_key=f'{ds.id}_copy').dataset
    ds2_usentry = default_sync_usm_per_test.get_by_id(ds.id)
    assert isinstance(ds2_usentry, UsDataset)
    new_fields = ds2_usentry.data.result_schema.fields.copy()
    new_fields[field_index] = new_fields[field_index].clone(title='HELLO FROM CACHE')
    ds_editor = DatasetComponentEditor(dataset=ds2_usentry)
    ds_editor.set_result_schema(new_fields)

    field = ds2.result_schema[field_index]

    key = create_mutation_key(dataset=ds_usentry, updates=updates)
    # Check no saved key in cache
    assert loop.run_until_complete(_get_from_cache(mutation_cache, dataset=ds_usentry, key=key)) is None
    # Save to cache dataset with difference from original waiting result
    loop.run_until_complete(_save_to_cache(mutation_cache, ds_usentry, ds2_usentry, key=key))
    # Check now key is saved
    assert loop.run_until_complete(_get_from_cache(mutation_cache, dataset=ds_usentry, key=key)) is not None
    # Make a request
    resp = data_api_v2.get_result(dataset=ds, updates=updates, fields=[field])
    assert resp.status_code == 200
    # Check that the result equals not real value waited from get_result but equals our incorrect result
    assert resp.json.get('fields')[0].get('title') == 'HELLO FROM CACHE'


def test_date_param(
    api_v1, data_api_v2_with_mutation_cache, mutation_cache, loop, dataset_id, default_sync_usm_per_test,
):
    data_api_v2 = data_api_v2_with_mutation_cache
    ds = add_parameters_to_dataset(
        api_v1=api_v1,
        dataset_id=dataset_id,
        parameters={
            'date_param': (
                DateParameterValue('2021-12-14'),
                None,
            ),
        }
    )
    ds.result_schema['formula_param'] = ds.field(formula='[date_param]')
    ds = api_v1.apply_updates(dataset=ds).dataset
    ds = api_v1.save_dataset(dataset=ds).dataset

    updates = [
        {
            'action': 'update_field',
            'field': {
                'calc_mode': 'parameter',
                'guid': 'date_param',
                'title': 'new title',
                'default_value': '2022-04-25',
                'cast': 'date',
            },
            'order_index': 0
        }
    ]
    field = ds.find_field('formula_param')
    resp1 = data_api_v2.get_result(dataset=ds, updates=updates, fields=[field])
    assert resp1.status_code == 200
    resp2 = data_api_v2.get_result(dataset=ds, updates=updates, fields=[field])
    assert resp2.status_code == 200
    assert resp1.json == resp2.json


def test_cached_response_is_equal(data_api_v2_with_mutation_cache, api_v1, dataset_id):
    data_api_v2 = data_api_v2_with_mutation_cache
    ds = api_v1.load_dataset(dataset=Dataset(id=dataset_id)).dataset
    ds = api_v1.save_dataset(dataset=ds).dataset
    updates = get_updates(ds.result_schema[field_index].id)
    field = ds.result_schema[field_index]
    resp1 = data_api_v2.get_result(dataset=ds, updates=updates, fields=[field])
    assert resp1.status_code == 200
    resp2 = data_api_v2.get_result(dataset=ds, updates=updates, fields=[field])
    assert resp2.status_code == 200
    assert resp1.json == resp2.json


def test_new_update_no_cache(
    data_api_v2_test_mutation_cache, api_v1, dataset_id, mutation_cache, loop, default_sync_usm_per_test
):
    """
    Tests that calling `/result` twice with the same updates produces the same result
    with and without mutation caching
    """

    data_api_v2, cache_on = data_api_v2_test_mutation_cache

    updates_1 = [{
        "action": "add_field",
        "field": {
            "calc_mode": "formula",
            "formula": "'hiiiii'",
            "guid": "guid_1",
            "title": "Field 1",
        }
    }]

    updates_2 = updates_1 + [{
        "action": "add_field",
        "field": {
            "calc_mode": "formula",
            "formula": "'Hi 2'",
            "guid": "guid_2",
            "title": "Field 2",
        }
    }]

    ds = api_v1.load_dataset(dataset=Dataset(id=dataset_id)).dataset
    ds = api_v1.save_dataset(dataset=ds).dataset
    ds_usentry = default_sync_usm_per_test.get_by_id(ds.id)
    fields_1 = api_v1.apply_updates(dataset=ds, updates=updates_1).dataset.result_schema
    fields_2 = api_v1.apply_updates(dataset=ds, updates=updates_2).dataset.result_schema

    resp_1_1 = data_api_v2.get_result(dataset=ds, updates=updates_1, fields=fields_1)
    if cache_on:  # check no cache for second update
        key_1 = create_mutation_key(dataset=ds_usentry, updates=updates_1)
        key_2 = create_mutation_key(dataset=ds_usentry, updates=updates_2)
        assert key_1 != key_2
        cached_ds_1 = loop.run_until_complete(_get_from_cache(mutation_cache, dataset=ds_usentry, key=key_1))
        cached_ds_2 = loop.run_until_complete(_get_from_cache(mutation_cache, dataset=ds_usentry, key=key_2))
        assert cached_ds_1 is not None
        assert cached_ds_2 is None
    resp_2_1 = data_api_v2.get_result(dataset=ds, updates=updates_2, fields=fields_2)
    resp_1_1_json = sort_superstore_json(resp_1_1.json, 15)
    resp_2_1_json = sort_superstore_json(resp_2_1.json, 16)
    assert resp_1_1 != resp_2_1
    assert resp_1_1_json != resp_2_1_json

    if cache_on:  # check cache saved
        key_1 = create_mutation_key(dataset=ds_usentry, updates=updates_1)
        key_2 = create_mutation_key(dataset=ds_usentry, updates=updates_2)
        assert key_1 != key_2
        cached_ds_1 = loop.run_until_complete(_get_from_cache(mutation_cache, dataset=ds_usentry, key=key_1))
        cached_ds_2 = loop.run_until_complete(_get_from_cache(mutation_cache, dataset=ds_usentry, key=key_2))
        assert cached_ds_1 is not None
        assert cached_ds_2 is not None
        assert cached_ds_1 != cached_ds_2

    resp_1_2 = data_api_v2.get_result(dataset=ds, updates=updates_1, fields=fields_1)
    resp_2_2 = data_api_v2.get_result(dataset=ds, updates=updates_2, fields=fields_2)
    resp_1_2_json = sort_superstore_json(resp_1_2.json, 15)
    resp_2_2_json = sort_superstore_json(resp_2_2.json, 16)
    assert resp_1_2 != resp_2_2
    assert resp_1_2_json != resp_2_2_json

    assert resp_1_1_json == resp_1_2_json
    assert resp_2_1_json == resp_2_2_json


def test_update_same_field(
    data_api_v2_test_mutation_cache, api_v1, dataset_id, mutation_cache, loop, default_sync_usm_per_test
):
    """
    Tests that calling `/result` with a slightly different update does not use cache
    and results are the same when called twice
    """

    data_api_v2, cache_on = data_api_v2_test_mutation_cache

    ds = api_v1.load_dataset(dataset=Dataset(id=dataset_id)).dataset
    ds = api_v1.save_dataset(dataset=ds).dataset
    ds_usentry = default_sync_usm_per_test.get_by_id(ds.id)
    guid = ds.result_schema[field_index].id

    updates_1 = [{
        "action": "update_field",
        "field": {
            "guid": guid,
            "title": "Field update 1",
        }
    }]

    updates_2 = [{
        "action": "update_field",
        "field": {
            "guid": guid,
            "title": "Field update 2",
        }
    }]

    fields_1 = api_v1.apply_updates(dataset=ds, updates=updates_1).dataset.result_schema
    fields_2 = api_v1.apply_updates(dataset=ds, updates=updates_2).dataset.result_schema

    resp_1_1 = data_api_v2.get_result(dataset=ds, updates=updates_1, fields=fields_1)
    if cache_on:  # check no cache for second update
        key_1 = create_mutation_key(dataset=ds_usentry, updates=updates_1)
        key_2 = create_mutation_key(dataset=ds_usentry, updates=updates_2)
        assert key_1 != key_2
        cached_ds_1 = loop.run_until_complete(_get_from_cache(mutation_cache, dataset=ds_usentry, key=key_1))
        cached_ds_2 = loop.run_until_complete(_get_from_cache(mutation_cache, dataset=ds_usentry, key=key_2))
        assert cached_ds_1 is not None
        assert cached_ds_2 is None
    resp_2_1 = data_api_v2.get_result(dataset=ds, updates=updates_2, fields=fields_2)
    resp_1_1_json = sort_superstore_json(resp_1_1.json, 14)
    resp_2_1_json = sort_superstore_json(resp_2_1.json, 14)
    assert resp_1_1 != resp_2_1
    assert resp_1_1_json != resp_2_1_json

    if cache_on:  # check cache saved
        key_1 = create_mutation_key(dataset=ds_usentry, updates=updates_1)
        key_2 = create_mutation_key(dataset=ds_usentry, updates=updates_2)
        assert key_1 != key_2
        cached_ds_1 = loop.run_until_complete(_get_from_cache(mutation_cache, dataset=ds_usentry, key=key_1))
        cached_ds_2 = loop.run_until_complete(_get_from_cache(mutation_cache, dataset=ds_usentry, key=key_2))
        assert cached_ds_1 is not None
        assert cached_ds_2 is not None
        assert cached_ds_1 != cached_ds_2

    resp_1_2 = data_api_v2.get_result(dataset=ds, updates=updates_1, fields=fields_1)
    resp_2_2 = data_api_v2.get_result(dataset=ds, updates=updates_2, fields=fields_2)
    resp_1_2_json = sort_superstore_json(resp_1_2.json, 14)
    resp_2_2_json = sort_superstore_json(resp_2_2.json, 14)
    assert resp_1_2 != resp_2_2
    assert resp_1_2_json != resp_2_2_json

    assert resp_1_1_json == resp_1_2_json
    assert resp_2_1_json == resp_2_2_json


def sort_superstore_json(json, row_id_index):
    print('Sorting json\n')
    rows = json['result_data'][0]['rows']
    json['result_data'][0]['rows'] = sorted(rows, key=lambda r: int(r['data'][row_id_index]))
    keys = [str(r['data'][row_id_index]) for r in json['result_data'][0]['rows']]
    print(','.join(keys))
    return json
