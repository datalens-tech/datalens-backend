from __future__ import annotations

import json
import uuid

from sqlalchemy_metrika_api.api_info.metrika import MetrikaApiCounterSource

from bi_api_client.dsmaker.primitives import Dataset
from bi_api_client.dsmaker.shortcuts.result_data import get_data_rows
from bi_api_client.dsmaker.shortcuts.range_data import get_range_values

from bi_constants.enums import AggregationFunction, FieldType

from bi_connector_metrica.core.constants import SOURCE_TYPE_METRICA_API


def test_metrica_connection(client, env_param_getter):
    dir_path = 'unit_tests/connections'
    conn_name = f'metrica_{uuid.uuid4().hex}'

    valid_counter = '44147844,44147844'
    invalid_counters = ['44147844,-44147844', '44147844,asdf']

    conn_create_data = {
        'type': 'metrika_api',
        'dir_path': dir_path,
        'name': conn_name,
        'accuracy': 0.01,
        'counter_id': ...,
        'token': env_param_getter.get_str_value('METRIKA_OAUTH'),
    }

    for counter in invalid_counters:
        conn_create_data['counter_id'] = counter
        resp = client.post(
            '/api/v1/connections',
            data=json.dumps(conn_create_data),
            content_type='application/json'
        )
        assert resp.status_code == 400, resp.json

    conn_create_data['counter_id'] = valid_counter
    resp = client.post(
        '/api/v1/connections',
        data=json.dumps(conn_create_data),
        content_type='application/json'
    )
    assert resp.status_code == 200, resp.json
    conn_id = resp.json['id']

    resp = client.get(f'/api/v1/connections/{conn_id}')
    assert resp.status_code == 200
    assert resp.json['db_type'] == 'metrika_api'

    update_resp = client.put(
        '/api/v1/connections/{}'.format(conn_id),
        data=json.dumps({'counter_id': '44147844', 'token': env_param_getter.get_str_value('METRIKA_OAUTH')}),
        content_type='application/json'
    )
    assert update_resp.status_code == 200, update_resp.json

    update_resp = client.put(
        '/api/v1/connections/{}'.format(conn_id),
        data=json.dumps({'token': 'asdf'}),
        content_type='application/json'
    )
    assert update_resp.status_code == 200, update_resp.json


def test_metrica_api_dataset(api_v1, data_api_v2, metrika_api_connection_id):
    data_api = data_api_v2
    connection_id = metrika_api_connection_id
    ds = Dataset()
    ds.sources['source_1'] = ds.source(
        connection_id=connection_id,
        source_type=SOURCE_TYPE_METRICA_API,
        parameters=dict(
            db_name=MetrikaApiCounterSource.hits.name,
        ),
    )
    ds.source_avatars['avatar_1'] = ds.sources['source_1'].avatar()
    ds_resp = api_v1.apply_updates(dataset=ds)
    ds = ds_resp.dataset

    options = ds_resp.json['options']
    assert all(not field['aggregations'] for field in options['data_types']['items'])

    ds = api_v1.save_dataset(dataset=ds).dataset

    # preview is not available for Metrica

    # result
    result_resp = data_api.get_result(
        dataset=ds,
        limit=3,
        fields=[
            ds.find_field(title='Дата просмотра'),
            ds.find_field(title='Просмотров в минуту'),
        ],
        filters=[ds.find_field(title='Дата просмотра').filter(
            'BETWEEN',
            values=['2019-12-01', '2019-12-07 12:00:00']
        )],
        order_by=[ds.find_field(title='Просмотров в минуту')],
    )
    data_rows = get_data_rows(result_resp)
    assert len(data_rows) == 3

    ds.result_schema['new field'] = ds.field(
        avatar_id=ds.source_avatars[0].id,
        source='ym:pv:pageviewsPerMinute',
        aggregation=AggregationFunction.none,
        title='new field'
    )
    ds_resp = api_v1.apply_updates(dataset=ds)
    resp_result_schema = ds_resp.json['dataset']['result_schema']
    assert resp_result_schema[0]['title'] == 'new field'
    assert resp_result_schema[0]['type'] == FieldType.MEASURE.name


def test_metrica_api_value_range(api_v1, data_api_v2, metrika_api_connection_id):
    data_api = data_api_v2
    connection_id = metrika_api_connection_id
    ds = Dataset()
    ds.sources['source_1'] = ds.source(
        connection_id=connection_id,
        source_type=SOURCE_TYPE_METRICA_API,
        parameters=dict(
            db_name=MetrikaApiCounterSource.hits.name,
        ),
    )
    ds.source_avatars['avatar_1'] = ds.sources['source_1'].avatar()
    ds = api_v1.apply_updates(dataset=ds).dataset
    ds = api_v1.save_dataset(dataset=ds).dataset

    min_value, max_value = get_range_values(data_api.get_value_range(
        dataset=ds,
        field=ds.find_field(title='Дата просмотра'),
    ))

    assert min_value is not None and max_value is not None


def test_metrica_concat_validation(api_v1, metrika_api_connection_id):
    connection_id = metrika_api_connection_id
    ds = Dataset()
    ds.sources['source_1'] = ds.source(
        connection_id=connection_id,
        source_type=SOURCE_TYPE_METRICA_API,
        parameters=dict(
            db_name=MetrikaApiCounterSource.hits.name,
        ),
    )
    ds.source_avatars['avatar_1'] = ds.sources['source_1'].avatar()
    ds = api_v1.apply_updates(dataset=ds).dataset
    ds = api_v1.save_dataset(dataset=ds).dataset

    ds.result_schema['Test'] = ds.field(formula='CONCAT("TEST1 ", "test2")')
    ds_resp = api_v1.apply_updates(dataset=ds, fail_ok=True)
    assert ds_resp.status_code == 400, ds_resp.json
    assert ds_resp.json['code'] == 'ERR.DS_API.VALIDATION.ERROR'
