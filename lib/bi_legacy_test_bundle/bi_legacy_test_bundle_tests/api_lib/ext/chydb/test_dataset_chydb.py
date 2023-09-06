from __future__ import annotations

import json

import pytest

from bi_constants.enums import CreateDSFrom

from bi_api_client.dsmaker.primitives import Dataset

from bi_legacy_test_bundle_tests.api_lib.utils import get_result_schema, get_random_str


@pytest.fixture
def chydb_connection_params(app, yt_token):
    return {
        'name': 'chydb_test_{}'.format(get_random_str()),
        'type': 'chydb',
        'token': yt_token,

        # TODO: `**bi_core_testing.chydb_test_connection_params(),`
        'host': 'ydb-clickhouse.yandex.net',
        'port': 8123,
        'secure': 'off',

        'default_ydb_database': 'whatever value for YDB database',
        'default_ydb_cluster': 'whatever value for YDB cluster',
    }


@pytest.fixture
def chydb_connection_id(app, client, chydb_connection_params, request):
    resp = client.post(
        '/api/v1/connections',
        data=json.dumps(chydb_connection_params),
        content_type='application/json'
    )
    assert resp.status_code == 200, resp.json
    conn_id = resp.json['id']

    def teardown():
        client.delete('/api/v1/connections/{}'.format(conn_id))

    request.addfinalizer(teardown)

    return conn_id


def test_chydb_connection_sources(client, chydb_connection_id):
    """
    Ensure the saved default_... parameters are returned in the source.
    """
    conn_id = chydb_connection_id
    resp = client.get(f'/api/v1/connections/{conn_id}/info/sources')
    assert resp.status_code == 200, resp.json
    resp_data = resp.json
    assert len(resp_data['freeform_sources']) == 2, resp_data
    src_by_st = {item['source_type']: item for item in resp_data['freeform_sources'] + resp_data['sources']}
    # r_s_p: response_source_parameters, i.e. response_data__data_source__parameters
    r_s_p = src_by_st['CHYDB_TABLE']['parameters']
    assert r_s_p['ydb_database'] == 'whatever value for YDB database'
    assert r_s_p['ydb_cluster'] == 'whatever value for YDB cluster'


@pytest.fixture()
def chydb_dataset_legacy(request, client, api_v1, chydb_connection_id):
    conn_id = chydb_connection_id

    ds = Dataset()
    ds.sources['source_1'] = ds.source(
        connection_id=conn_id,
        source_type=CreateDSFrom.CHYDB_TABLE,
        parameters=dict(
            table_name='/ru-prestable/home/hhell/mydb/test_table_d',
        ))

    ds.source_avatars['avatar_1'] = ds.sources['source_1'].avatar()
    ds = api_v1.apply_updates(dataset=ds).dataset
    ds_resp = api_v1.save_dataset(dataset=ds, preview=False)
    assert ds_resp.status_code == 200
    ds = ds_resp.dataset

    def teardown(ds_id=ds.id):
        client.delete('/api/v1/datasets/{}'.format(ds_id))

    request.addfinalizer(teardown)

    return ds


@pytest.fixture()
def chydb_dataset(request, client, api_v1, chydb_connection_id):
    conn_id = chydb_connection_id

    ds = Dataset()
    ds.sources['source_1'] = ds.source(
        connection_id=conn_id,
        source_type=CreateDSFrom.CHYDB_TABLE,
        parameters=dict(
            ydb_cluster='ru',
            ydb_database='/ru/home/hhell/mydb',
            table_name='some_dir/test_table_e',
        ))
    assert ds.sources['source_1'].parameters['ydb_cluster'] == 'ru'

    ds.source_avatars['avatar_1'] = ds.sources['source_1'].avatar()
    assert ds.sources['source_1'].parameters['ydb_cluster'] == 'ru'
    ds = api_v1.apply_updates(dataset=ds).dataset
    assert ds.sources['source_1'].parameters['ydb_cluster'] == 'ru'
    ds_resp = api_v1.save_dataset(dataset=ds, preview=False)
    assert ds_resp.status_code == 200
    ds = ds_resp.dataset
    assert ds.sources['source_1'].parameters['ydb_cluster'] == 'ru'

    def teardown(ds_id=ds.id):
        client.delete('/api/v1/datasets/{}'.format(ds_id))

    request.addfinalizer(teardown)

    return ds


def test_dataset_result_chydb_legacy(client, data_api_v1, chydb_dataset_legacy):
    dataset_id = chydb_dataset_legacy.id

    result_schema = get_result_schema(client, dataset_id)
    # TODO?: all columns
    columns = [result_schema[0]['guid']]
    response = data_api_v1.get_response_for_dataset_result(
        dataset_id=dataset_id,
        raw_body={'columns': columns, 'limit': 3},
    )
    assert response.status_code == 200


def test_dataset_result_chydb(client, data_api_v1, chydb_dataset):
    dataset_id = chydb_dataset.id

    result_schema = get_result_schema(client, dataset_id)
    # TODO?: all columns
    columns = [result_schema[0]['guid']]
    response = data_api_v1.get_response_for_dataset_result(
        dataset_id=dataset_id,
        raw_body={'columns': columns, 'limit': 3},
    )
    assert response.status_code == 200
