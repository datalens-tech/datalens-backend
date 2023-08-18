from __future__ import annotations

from http import HTTPStatus

import pytest

from bi_constants.enums import BIType

from bi_api_client.dsmaker.primitives import Dataset


def test_update_id_field(api_v1, static_dataset_id):
    ds = api_v1.load_dataset(dataset=Dataset(id=static_dataset_id)).dataset

    field_index = 2
    old_field = ds.result_schema[field_index]
    ds_resp = api_v1.apply_updates(dataset=ds, updates=[
        {
            'action': 'update_field',
            'field': {
                'guid': old_field.id,
                'new_id': 'new_guid',
            },
        }
    ])
    assert ds_resp.status_code == HTTPStatus.OK, ds_resp.response_errors
    ds = ds_resp.dataset

    new_field = ds.result_schema[field_index]
    assert new_field.id == 'new_guid'


def test_update_id_field_collision(api_v1, static_dataset_id):
    ds = api_v1.load_dataset(dataset=Dataset(id=static_dataset_id)).dataset

    first_field, second_field = [f for f in ds.result_schema if f.data_type == BIType.date][0:2]
    with pytest.raises(AssertionError):
        api_v1.apply_updates(dataset=ds, updates=[
            {
                'action': 'update_field',
                'field': {
                    'guid': first_field.id,
                    'new_id': second_field.id,
                },
            }
        ])


@pytest.mark.parametrize(
    'new_id',
    [
        '',
        'very_very_very_very_very_very_very_very_long_field_id',
        'UPPER_FIELD_ID',
        'Айди поля',
    ],
)
def test_update_non_valid_id_field(api_v1, static_dataset_id, new_id):
    ds = api_v1.load_dataset(dataset=Dataset(id=static_dataset_id)).dataset

    old_field = ds.result_schema[0]
    with pytest.raises(AssertionError):
        api_v1.apply_updates(dataset=ds, updates=[
            {
                'action': 'update_field',
                'field': {
                    'guid': old_field.id,
                    'new_id': new_id,
                },
            }
        ])


def test_update_id_field_with_component_errors(api_v1, static_dataset_id):
    ds = api_v1.load_dataset(dataset=Dataset(id=static_dataset_id)).dataset
    a_field = [f for f in ds.result_schema if f.cast == BIType.float][0]
    ds_resp = api_v1.apply_updates(dataset=ds, updates=[
        {
            'action': 'add_field',
            'field': {
                'title': 'Doubled {}'.format(a_field.title),
                'calc_mode': 'formula',
                'formula': '[{}] * 2'.format(a_field.title),
            },
        }
    ])
    assert ds_resp.status_code == HTTPStatus.OK
    ds = ds_resp.dataset

    b_field = [f for f in ds.result_schema if f.title == 'Doubled {}'.format(a_field.title)][0]
    ds_resp = api_v1.apply_updates(dataset=ds, updates=[
        {
            'action': 'update_field',
            'field': {
                'guid': b_field.id,
                'formula': 'GREATEST([{}] * 2, '.format(a_field.title),
            },
        }
    ], fail_ok=True)
    assert ds_resp.status_code == HTTPStatus.BAD_REQUEST
    ds = ds_resp.dataset

    assert len(ds.component_errors.items) == 1
    component_errors_ids = [item.id for item in ds.component_errors.items]
    assert b_field.id in component_errors_ids

    ds_resp = api_v1.apply_updates(dataset=ds, updates=[
        {
            'action': 'update_field',
            'field': {
                'guid': b_field.id,
                'new_id': 'new_guid',
            },
        }
    ], fail_ok=True)
    assert ds_resp.status_code == HTTPStatus.BAD_REQUEST
    ds = ds_resp.dataset

    assert len(ds.component_errors.items) == 1
    component_errors_ids = [item.id for item in ds.component_errors.items]
    assert b_field.id not in component_errors_ids
    assert 'new_guid' in component_errors_ids
