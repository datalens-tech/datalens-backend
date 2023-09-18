from typing import Any

import pytest
import attr

from dl_api_lib.api_common.update_dataset_mutation_key import UpdateDatasetMutationKey
from dl_api_lib.request_model.data import FieldAction
from dl_api_lib.schemas.action import ActionSchema

TEST_DATASET_REVISION_ID = '123'
TEST_DATASET_ANOTHER_REVISION_ID = '321'


def test_empty_mutation_keys():
    mutation_key_empty = UpdateDatasetMutationKey.create(TEST_DATASET_REVISION_ID, [])
    mutation_key_empty_copy = UpdateDatasetMutationKey.create(TEST_DATASET_REVISION_ID, [])
    mutation_key_empty_different_dataset = UpdateDatasetMutationKey.create(TEST_DATASET_ANOTHER_REVISION_ID, [])
    assert mutation_key_empty.get_hash() == mutation_key_empty_copy.get_hash()
    assert mutation_key_empty.get_hash() != mutation_key_empty_different_dataset.get_hash()


@pytest.fixture
def original_mutation_list() -> list[dict[str, Any]]:
    return [
        {
            "action": "add_field",
            "field": {
                "formula": "sum([Discount])*2",
                "cast": "float",
                "description": "",
                "source": "",
                "hidden": False,
                "aggregation": "none",
                "title": "title-11e2dd40-6739-11ec-9e02-a5fd2b33324c",
                "guid": "test",
                "calc_mode": "formula",
            },
            "debug_info": "merged-update"
        },
        {
            "action": "add_field",
            "field": {
                "valid": True,
                "calc_mode": "parameter",
                "source": "",
                "description": "",
                "title": "datetime_param2",
                "type": "DIMENSION",
                "aggregation": "none",
                "hidden": False,
                "guid": "datetime_param2",
                "cast": "datetime",
                "default_value": "2022-05-18T12:33:21.000+03:00"
            }
        },
    ]


@pytest.fixture
def original_mutation(original_mutation_list) -> list[FieldAction]:
    return ActionSchema(many=True).load(original_mutation_list)


@pytest.fixture
def original_mutation_shuffle_dicts(original_mutation_list) -> list[FieldAction]:
    return ActionSchema(many=True).load(original_mutation_list[::-1])


@pytest.fixture
def original_mutation_shuffle_keys(original_mutation_list) -> list[FieldAction]:
    shuffled = []
    for update in original_mutation_list:
        update['field'] = {k: v for k, v in list(update['field'].items())[::-1]}
        shuffled.append({k: v for k, v in list(update.items())[::-1]})
    return ActionSchema(many=True).load(shuffled)


def test_reordered_mutation_keys(
    original_mutation,
    original_mutation_shuffle_dicts,
    original_mutation_shuffle_keys,
):
    mutation_key_original = UpdateDatasetMutationKey.create(TEST_DATASET_REVISION_ID, original_mutation)
    mutation_key_shuffled_1 = UpdateDatasetMutationKey.create(TEST_DATASET_REVISION_ID, original_mutation_shuffle_dicts)
    mutation_key_shuffled_2 = UpdateDatasetMutationKey.create(TEST_DATASET_REVISION_ID, original_mutation_shuffle_keys)
    assert mutation_key_original.get_hash() == mutation_key_shuffled_1.get_hash()
    assert mutation_key_original.get_hash() == mutation_key_shuffled_2.get_hash()


@pytest.fixture
def mutation_add_one_dict(original_mutation_list) -> list[FieldAction]:
    return ActionSchema(many=True).load(
        original_mutation_list +
        [{
            "action": "add_field",
            "field": {
                "calc_mode": "formula",
                "aggregation": "none",
                "title": "title-1234d4a0-9309-11ec-9b5b-91d1cff8b10b",
                "guid": "test",
                "cast": "integer",
                "hidden": True,
                "description": "",
                "source": "",
                "formula": "[Profit]*2",
            }
        }]
    )


@pytest.fixture
def mutation_add_one_key(original_mutation_list) -> list[FieldAction]:
    original_mutation_list[1]['field']['avatar_id'] = '132132'
    return ActionSchema(many=True).load(original_mutation_list)


@pytest.fixture
def mutation_different_value(original_mutation_list) -> list[FieldAction]:
    original_mutation_list[0]['field']['description'] = 'extra description'
    return ActionSchema(many=True).load(original_mutation_list)


def test_cached_updates_mutation_keys(
    original_mutation,
    mutation_add_one_dict,
    mutation_add_one_key,
    mutation_different_value,
):
    mutation_key_original = UpdateDatasetMutationKey.create(TEST_DATASET_REVISION_ID, original_mutation)
    mutation_key_more_dicts = UpdateDatasetMutationKey.create(TEST_DATASET_REVISION_ID, mutation_add_one_dict)
    mutation_key_more_keys = UpdateDatasetMutationKey.create(TEST_DATASET_REVISION_ID, mutation_add_one_key)
    mutation_key_different_value = UpdateDatasetMutationKey.create(TEST_DATASET_REVISION_ID, mutation_different_value)
    assert mutation_key_original.get_hash() != mutation_key_more_dicts.get_hash()
    assert mutation_key_original.get_hash() != mutation_key_more_keys.get_hash()
    assert mutation_key_original.get_hash() != mutation_key_different_value.get_hash()


def test_mutation_comparation_logic_and_collision(original_mutation):
    mutation_key_1 = UpdateDatasetMutationKey.create(TEST_DATASET_REVISION_ID, original_mutation)
    mutation_key_2 = UpdateDatasetMutationKey.create(TEST_DATASET_REVISION_ID, original_mutation)
    mutation_key_diff = UpdateDatasetMutationKey.create(TEST_DATASET_ANOTHER_REVISION_ID, original_mutation)
    assert mutation_key_1 == mutation_key_2
    assert mutation_key_1 != mutation_key_diff
    assert mutation_key_1 != 123
    mutation_collision = attr.evolve(mutation_key_diff, hash=mutation_key_1.get_hash())
    assert mutation_key_1.get_hash() == mutation_collision.get_hash()
    assert mutation_key_1.get_collision_tier_breaker() != mutation_collision.get_collision_tier_breaker()
    assert mutation_key_1 != mutation_collision
    assert mutation_key_diff != mutation_collision
