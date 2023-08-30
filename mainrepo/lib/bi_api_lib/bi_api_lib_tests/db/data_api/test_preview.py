from __future__ import annotations

import math

import pytest

from bi_api_client.dsmaker.primitives import Dataset
from bi_api_client.dsmaker.shortcuts.result_data import get_data_rows

from bi_api_lib_tests.utils import validate_schema, data_source_settings_from_table

CASES = [
    {
        'name': 'simple direct',
        'fields': [
            {
                'title': 'Discount',
                'calc_mode': 'direct',
                'cast': 'float',
                'source': 'Discount',
            }
        ],
        'checks': [
            (
                'all rows have 1 value',
                lambda d: all(len(r) == 1 for r in d)
            ),
        ]
    },
    {
        'name': 'double discount',
        'fields': [
            {
                'title': 'Discount',
                'calc_mode': 'direct',
                'cast': 'float',
                'source': 'Discount',
            },
            {
                'title': 'Doubled Discount',
                'calc_mode': 'formula',
                'formula': '[Discount] * 2',
                'cast': 'float',
            }
        ],
        'checks': [
            (
                'discount is doubled',
                lambda d: all(float_eq(float(r[0]) * 2, r[1]) for r in d)
            )
        ]
    },
    {
        'name': 'simple aggregation',
        'fields': [
            {
                'title': 'City',
                'calc_mode': 'direct',
                'cast': 'string',
                'source': 'City',
            },
            {
                'title': 'Discount',
                'calc_mode': 'direct',
                'cast': 'float',
                'source': 'Discount',
                'aggregation': 'avg',
            },
        ],
        'checks': [
            (
                'cities are unique',
                lambda d: len([r[0] for r in d]) == len(set(r[0] for r in d)) != 0
            ),
            (
                'avg(discount) are float',
                lambda d: all(float(r[1]) is not None for r in d)
            )
        ]
    },
    {
        'name': 'renamed field in formula 1',
        'fields': [
            {
                'title': 'Renamed Discount',
                'calc_mode': 'direct',
                'cast': 'float',
                'source': 'Discount',
            },
            {
                'title': 'Formula',
                'calc_mode': 'formula',
                'cast': 'float',
                'formula': '[Renamed Discount] * 2'
            }
        ],
        'checks': [
            (
                'formula field is correct',
                lambda d: all(float_eq(float(r[0]) * 2, r[1]) for r in d)
            )
        ]
    },
    {
        'name': 'only formula',
        'fields': [
            {
                'title': 'Customer Count',
                'calc_mode': 'direct',
                'aggregation': 'countunique',
                'cast': 'string',
                'source': 'Customer ID',
            },
            {
                'title': 'Sales Sum',
                'calc_mode': 'direct',
                'aggregation': 'sum',
                'cast': 'integer',
                'source': 'Sales',
            },
            {
                'title': 'Sales per Cust',
                'calc_mode': 'formula',
                'cast': 'float',
                'formula': '[Sales Sum]/[Customer Count]'
            }
        ],
        'checks': [
            (
                'length',
                lambda d: len(d) == 1
            )
        ]
    },
    {
        'name': 'formula using hidden field by non-native name',
        'fields': [
            {
                'title': 'Renamed Discount',
                'calc_mode': 'direct',
                'cast': 'float',
                'source': 'Discount',
                'hidden': True,
            },
            {
                'title': 'Discount as Formula',
                'calc_mode': 'formula',
                'cast': 'float',
                'formula': '[Renamed Discount]'
            },
            {
                'title': 'Double Discount',
                'calc_mode': 'formula',
                'cast': 'float',
                'formula': '[Renamed Discount] * 2.0'
            },
        ],
        'checks': [
            (
                'row length',
                lambda d: all(len(r) == 2 for r in d)
            ),
            (
                'formula field is correct',
                lambda d: all(float_eq(float(r[0]) * 2, r[1]) for r in d)
            ),
        ]
    },
    {
        'name': 'formula using parameter',
        'fields': [
            {
                'title': 'Discount',
                'calc_mode': 'direct',
                'cast': 'float',
                'source': 'Discount',
            },
            {
                'title': 'Multiplier',
                'calc_mode': 'parameter',
                'cast': 'float',
                'default_value': 2.0,
            },
            {
                'title': 'Multiplied Discount',
                'calc_mode': 'formula',
                'cast': 'float',
                'formula': '[Discount] * [Multiplier]'
            },
        ],
        'checks': [
            (
                'row length',
                lambda d: all(len(r) == 2 for r in d)
            ),
            (
                'formula field is correct',
                lambda d: all(float_eq(float(r[0]) * 2, r[1]) for r in d)
            ),
        ]
    },
]


def float_eq(f1, f2):
    return math.fabs(float(f1) - float(f2)) < 1e-6


def raise_for_status(response):
    assert response.status_code < 300, 'Response {}, {}'.format(
        response.status_code, getattr(response, 'json', 'empty body')
    )


@pytest.mark.parametrize('case', CASES, ids=[s['name'] for s in CASES])
def test_get_preview(api_v1, data_api_all_v, static_dataset_id, case):
    data_api = data_api_all_v
    ds = api_v1.load_dataset(dataset=Dataset(id=static_dataset_id)).dataset
    avatar_id = ds.source_avatars[0].id
    # Remove all existing fields from schema
    updates = [
        f.delete()
        for f in ds.result_schema
    ]
    # Add new ones
    for f in case['fields'][::-1]:
        upd = {
            'action': 'add_field',
            'field': dict(f, avatar_id=avatar_id) if f.get('formula') is not None else f
        }
        updates.append(upd)
    checks = case['checks']

    ds = api_v1.apply_updates(dataset=ds, updates=updates).dataset

    preview_resp = data_api.get_preview(dataset=ds)

    data = get_data_rows(preview_resp)
    for check_desc, check_function in checks:
        assert check_function(data), check_desc


def test_get_preview_with_bad_source(api_v1, data_api_v1, ch_data_source_settings, default_sync_usm, static_dataset_id):
    data_api = data_api_v1  # todo: change to all

    # ds = Dataset()
    ds = api_v1.load_dataset(dataset=Dataset(id=static_dataset_id)).dataset
    ds.sources[0].parameters["table_name"] += "_bad_table_name"
    api_v1.apply_updates(dataset=ds, fail_ok=True)
    preview_resp = data_api.get_preview(dataset=ds, fail_ok=True)

    assert preview_resp.status_code == 400
    assert "_bad_table_name" in preview_resp.json["message"], preview_resp.json


def test_preview_with_corrupted_source_avatar(client, data_api_v1, static_dataset_id):
    dataset_id = static_dataset_id
    response = validate_schema(client, dataset_id)
    dataset_data = response.json
    del dataset_data['dataset']['source_avatars'][0]['managed_by']

    response = data_api_v1.get_response_for_dataset_preview(
        dataset_id=None,
        raw_body=dataset_data,
    )
    assert response.status_code == 400


def test_preview_with_corrupted_source(client, data_api_v2, static_dataset_id):
    dataset_id = static_dataset_id
    response = validate_schema(client, dataset_id)
    dataset_data = response.json
    del dataset_data['dataset']['sources'][0]['managed_by']

    response = data_api_v2.get_response_for_dataset_preview(
        dataset_id=None,
        raw_body=dataset_data,
    )
    assert response.status_code == 400


def test_preview_multisource_dataset(api_v1, data_api_v2, two_clickhouse_tables, static_connection_id):
    data_api = data_api_v2
    table_1, table_2 = two_clickhouse_tables
    connection_id = static_connection_id
    ds = Dataset()
    ds.sources['source_1'] = ds.source(connection_id=connection_id, **data_source_settings_from_table(table_1))
    ds.sources['source_2'] = ds.source(connection_id=connection_id, **data_source_settings_from_table(table_2))
    ds.source_avatars['avatar_1'] = ds.sources['source_1'].avatar()
    ds.source_avatars['avatar_2'] = ds.sources['source_2'].avatar()
    ds.avatar_relations['relation_1'] = ds.source_avatars['avatar_1'].join(
        ds.source_avatars['avatar_2']
    ).on(
        ds.col('int_value') == ds.col('int_value')
    )
    ds = api_v1.apply_updates(dataset=ds).dataset

    preview_resp = data_api.get_preview(dataset=ds, limit=7)

    rows = get_data_rows(preview_resp)
    assert len(rows) == 7

    # TODO FIX: Uncomment when issue with query extraction will be resolved: https://st.yandex-team.ru/BI-1381
    # assert 'FROM (SELECT' in queries[0]

    ds = api_v1.save_dataset(dataset=ds).dataset

    preview_resp = data_api.get_preview(dataset=ds, limit=7)

    rows = get_data_rows(preview_resp)
    assert len(rows) == 7
    # TODO FIX: Uncomment when issue with query extraction will be resolved
    # assert 'FROM (SELECT' not in queries[0]  # no fallback to origin


def test_preview_fallback_switcher(api_v1, data_api_all_v, two_clickhouse_tables, static_connection_id):
    data_api = data_api_all_v
    table_1, table_2 = two_clickhouse_tables
    connection_id = static_connection_id
    ds = Dataset()
    ds.sources['source_1'] = ds.source(connection_id=connection_id, **data_source_settings_from_table(table_1))
    ds.source_avatars['avatar_1'] = ds.sources['source_1'].avatar()
    ds = api_v1.apply_updates(dataset=ds).dataset

    # check not saved
    preview_resp = data_api.get_preview(dataset=ds, limit=7)
    assert len(get_data_rows(preview_resp)) == 7

    # check saved and materialized
    ds = api_v1.save_dataset(dataset=ds).dataset
    preview_resp = data_api.get_preview(dataset=ds, limit=7)
    assert len(get_data_rows(preview_resp)) == 7

    ds.sources['source_2'] = ds.source(connection_id=connection_id, **data_source_settings_from_table(table_2))
    ds.source_avatars['avatar_2'] = ds.sources['source_2'].avatar()
    ds.avatar_relations['relation_1'] = ds.source_avatars['avatar_1'].join(
        ds.source_avatars['avatar_2']
    ).on(
        ds.col('int_value') == ds.col('int_value')
    )
    ds = api_v1.apply_updates(dataset=ds).dataset

    # with second source (not saved -> not fully materialized)
    preview_resp = data_api.get_preview(dataset=ds, limit=7)
    assert len(get_data_rows(preview_resp)) == 7

    ds = api_v1.save_dataset(dataset=ds).dataset

    # with second source (saved, but not yet fully materialized)
    preview_resp = data_api.get_preview(dataset=ds, limit=7)
    assert len(get_data_rows(preview_resp)) == 7


def test_preview_without_sources(api_v1, data_api_all_v):
    data_api = data_api_all_v
    ds = Dataset()
    ds = api_v1.apply_updates(dataset=ds).dataset

    preview_resp = data_api.get_preview(dataset=ds)
    assert len(get_data_rows(preview_resp)) == 0
