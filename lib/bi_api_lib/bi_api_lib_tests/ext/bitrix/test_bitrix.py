from __future__ import annotations

import json

import pytest

from bi_api_lib.connectors.bitrix_gds.schemas import BitrixPortalValidator
from bi_api_client.dsmaker.primitives import Dataset
from bi_api_client.dsmaker.shortcuts.dataset import add_formulas_to_dataset
from bi_testing.utils import guids_from_titles

from bi_api_lib_tests.utils import get_random_str, get_result_schema


@pytest.fixture
def bitrix_conn_params(bitrix_token):
    return dict(
        type='bitrix24',
        name='bitrix_test_{}'.format(get_random_str()),
        portal='gds.office.bitrix.ru',
        token=bitrix_token,
    )


@pytest.fixture
def bitrix_uf_conn_params(bitrix_uf_token):
    return dict(
        type='bitrix24',
        name='bitrix_test_{}'.format(get_random_str()),
        portal='serbul.bitrix24.ru',
        token=bitrix_uf_token,
    )


@pytest.fixture
def bitrix_smart_tables_conn_params(bitrix_smart_tables_token):
    return dict(
        type='bitrix24',
        name='bitrix_test_{}'.format(get_random_str()),
        portal='serbul.bitrix24.ru',
        token=bitrix_smart_tables_token,
    )


@pytest.mark.parametrize(
    'portal',
    [
        'https://hostname.com/path/to/exploit?username=root',
        'https://hostname.com',
        'hostname.com/path/to/exploit?username=root',
    ],
)
def test_portal_validation(client, portal):
    conn_params = dict(
        type='bitrix24',
        name='bitrix_test_{}'.format(get_random_str()),
        portal=portal,
        token='token',
    )

    resp = client.post(
        '/api/v1/connections',
        data=json.dumps(conn_params),
        content_type='application/json',
    )
    resp_data = resp.json
    assert resp.status_code == 400, resp_data
    assert resp_data['portal'][0] == BitrixPortalValidator.error


@pytest.fixture
def bitrix_conn_id(app, client, request, bitrix_conn_params):
    conn_params = bitrix_conn_params
    resp = client.post(
        '/api/v1/connections',
        data=json.dumps(conn_params),
        content_type='application/json',
    )
    assert resp.status_code == 200, resp.json
    conn_id = resp.json['id']

    def teardown():
        client.delete('/api/v1/connections/{}'.format(conn_id))

    request.addfinalizer(teardown)

    return conn_id


@pytest.fixture
def bitrix_uf_conn_id(app, client, request, bitrix_uf_conn_params):
    conn_params = bitrix_uf_conn_params
    resp = client.post(
        '/api/v1/connections',
        data=json.dumps(conn_params),
        content_type='application/json',
    )
    assert resp.status_code == 200, resp.json
    conn_id = resp.json['id']

    def teardown():
        client.delete('/api/v1/connections/{}'.format(conn_id))

    request.addfinalizer(teardown)

    return conn_id


@pytest.fixture
def bitrix_smart_tables_conn_id(app, client, request, bitrix_smart_tables_conn_params):
    conn_params = bitrix_smart_tables_conn_params
    resp = client.post(
        '/api/v1/connections',
        data=json.dumps(conn_params),
        content_type='application/json',
    )
    assert resp.status_code == 200, resp.json
    conn_id = resp.json['id']

    def teardown():
        client.delete('/api/v1/connections/{}'.format(conn_id))

    request.addfinalizer(teardown)

    return conn_id


def test_bitrix_cache_ttl_sec_override(client, api_v1, default_sync_usm, bitrix_conn_id):
    conn_id = bitrix_conn_id
    conn = client.get('/api/v1/connections/{}'.format(conn_id)).json
    assert conn['cache_ttl_sec'] is None

    cache_ttl_override = 100500
    conn_data = dict()
    conn_data['cache_ttl_sec'] = cache_ttl_override
    resp = client.put(
        '/api/v1/connections/{}'.format(conn_id),
        data=json.dumps(conn_data),
        content_type='application/json'
    )
    assert resp.status_code == 200, resp.json

    resp = client.get(f'/api/v1/connections/{conn_id}')
    assert resp.status_code == 200
    resp_data = resp.json
    assert resp_data['cache_ttl_sec'] == cache_ttl_override, resp_data


def test_bitrix_portal_override(client, api_v1, default_sync_usm, bitrix_conn_id):
    conn_id = bitrix_conn_id
    conn = client.get('/api/v1/connections/{}'.format(conn_id)).json
    assert conn['portal'] == 'gds.office.bitrix.ru'

    new_portal = 'мойБитрикс24.рф'
    conn_data = dict()
    conn_data['portal'] = new_portal
    resp = client.put(
        '/api/v1/connections/{}'.format(conn_id),
        data=json.dumps(conn_data),
        content_type='application/json'
    )
    assert resp.status_code == 200, resp.json

    resp = client.get(f'/api/v1/connections/{conn_id}')
    assert resp.status_code == 200
    resp_data = resp.json
    assert resp_data['portal'] == new_portal, resp_data


def test_bitrix_conn_test(client, api_v1, default_sync_usm, bitrix_conn_params):
    conn_params = bitrix_conn_params
    resp = client.post(
        '/api/v1/connections/test_connection_params',
        data=json.dumps(conn_params),
        content_type='application/json',
    )
    assert resp.status_code == 200, resp.json


def test_bitrix_conn_test_error(client, bitrix_conn_params):
    conn_params = bitrix_conn_params
    resp = client.post(
        '/api/v1/connections/test_connection_params',
        data=json.dumps(dict(
            conn_params,
            portal='some_portal',
        )),
        content_type='application/json',
    )
    assert resp.status_code == 400, resp.json


@pytest.fixture
def bitrix_dataset(client, api_v1, request, bitrix_conn_id):
    conn_id = bitrix_conn_id

    resp = client.get(f'/api/v1/connections/{conn_id}/info/sources')
    assert resp.status_code == 200, resp.json
    source_cfg = dict()
    for source in resp.json['sources']:
        if source['title'] == 'crm_deal':
            source_cfg = source
            break
    source_cfg_keys = {'source_type', 'title', 'connection_id', 'parameters'}
    source_cfg_clean = {key: val for key, val in source_cfg.items() if key in source_cfg_keys}

    ds = Dataset()
    ds.sources['source_1'] = ds.source(**source_cfg_clean)
    ds.source_avatars['avatar_1'] = ds.sources['source_1'].avatar()

    ds = api_v1.apply_updates(dataset=ds).dataset
    ds = api_v1.save_dataset(dataset=ds, preview=False).dataset

    def teardown(ds_id=ds.id):
        client.delete('/api/v1/datasets/{}'.format(ds_id))

    request.addfinalizer(teardown)

    return ds


def test_bitrix_add_formula(client, data_api_v1, api_v1, bitrix_dataset):
    ds_id = bitrix_dataset.id

    _ = add_formulas_to_dataset(api_v1=api_v1, dataset_id=ds_id, formulas={
        'IDs Sum': 'SUM([ID])',
    })

    preview_resp = data_api_v1.get_response_for_dataset_preview(
        dataset_id=ds_id,
    )
    assert preview_resp.status_code == 200, preview_resp.json


def test_bitrix_result(client, data_api_v1, bitrix_dataset):
    ds_id = bitrix_dataset.id
    result_schema = get_result_schema(client, ds_id)
    columns = [col['guid'] for col in result_schema]
    response = data_api_v1.get_response_for_dataset_result(
        dataset_id=ds_id,
        raw_body=dict(columns=columns, limit=3),
    )
    assert response.status_code == 200
    rd = response.json
    types = rd['result']['data']['Type'][1][1]
    assert len(types) == len(columns)


def test_bitrix_result_distinct(client, data_api_v1, bitrix_dataset):
    ds_id = bitrix_dataset.id
    result_schema = get_result_schema(client, ds_id)
    req_data = {
        'columns': guids_from_titles(result_schema, ['ASSIGNED_BY_NAME']),
        'group_by': guids_from_titles(result_schema, ['ASSIGNED_BY_NAME']),
    }

    response = data_api_v1.get_response_for_dataset_result(
        dataset_id=ds_id,
        raw_body=req_data,
    )

    assert response.status_code == 200
    rd = response.json
    created_by = [x[0] for x in rd['result']['data']['Data']]
    assert len(created_by) == len(set(created_by))


def test_bitrix_result_filtration(client, data_api_v1, bitrix_dataset):
    ds_id = bitrix_dataset.id
    result_schema = get_result_schema(client, ds_id)
    req_data = {
        'columns': guids_from_titles(result_schema, ['ID', 'ASSIGNED_BY_NAME']),
        'where': [{
            'column': guids_from_titles(result_schema, ['ASSIGNED_BY_NAME'])[0],
            'operation': 'EQ',
            'values': ['Роман Петров'],
        }]
    }

    response = data_api_v1.get_response_for_dataset_result(
        dataset_id=ds_id,
        raw_body=req_data,
    )

    assert response.status_code == 200
    rd = response.json
    assert all(item[1] == 'Роман Петров' for item in rd['result']['data']['Data'])


def test_bitrix_result_date_filtration(client, data_api_v1, api_v1, bitrix_dataset):
    ds_id = bitrix_dataset.id
    result_schema = get_result_schema(client, ds_id)
    req_data = {
        'columns': guids_from_titles(result_schema, ['ID', 'DATE_CREATE', 'DATE_MODIFY', 'ASSIGNED_BY_NAME']),
        'where': [{
            'column': guids_from_titles(result_schema, ['DATE_CREATE'])[0],
            'operation': 'BETWEEN',
            'values': ['2021-10-01', '2021-10-31'],
        }, {
            'column': guids_from_titles(result_schema, ['DATE_MODIFY'])[0],
            'operation': 'GT',
            'values': ['2021-11-01'],
        }, {
            'column': guids_from_titles(result_schema, ['ASSIGNED_BY_NAME'])[0],
            'operation': 'EQ',
            'values': ['Роман Петров'],
        }]
    }

    response = data_api_v1.get_response_for_dataset_result(
        dataset_id=ds_id,
        raw_body=req_data,
    )

    assert response.status_code == 200

    _ = add_formulas_to_dataset(api_v1=api_v1, dataset_id=ds_id, formulas={
        'CREATE DATE': 'DATE([DATE_CREATE])',
    })

    result_schema = get_result_schema(client, ds_id)
    req_data = {
        'columns': guids_from_titles(result_schema, ['ID', 'CREATE DATE']),
        'where': [{
            'column': guids_from_titles(result_schema, ['CREATE DATE'])[0],
            'operation': 'BETWEEN',
            'values': ['2021-10-01', '2021-10-31'],
        }]
    }

    response = data_api_v1.get_response_for_dataset_result(
        dataset_id=ds_id,
        raw_body=req_data,
    )
    assert response.status_code == 200


def test_bitrix_result_whith_caches(client, data_api_v1_with_caches, bitrix_dataset):
    data_api = data_api_v1_with_caches
    ds_id = bitrix_dataset.id
    result_schema = get_result_schema(client, ds_id)
    columns = [col['guid'] for col in result_schema]
    response = data_api.get_response_for_dataset_result(
        dataset_id=ds_id,
        raw_body=dict(columns=columns, limit=3),
    )
    assert response.status_code == 200

    req_data = {
        'columns': guids_from_titles(result_schema, ['ID', 'ASSIGNED_BY_NAME']),
        'where': [{
            'column': guids_from_titles(result_schema, ['ASSIGNED_BY_NAME'])[0],
            'operation': 'EQ',
            'values': ['Роман Петров'],
        }]
    }
    response = data_api.get_response_for_dataset_result(
        dataset_id=ds_id,
        raw_body=req_data,
    )

    assert response.status_code == 200


def test_bitrix_sources(client, api_v1, data_api_v1, bitrix_conn_id):
    conn_id = bitrix_conn_id

    resp = client.get(f'/api/v1/connections/{conn_id}/info/sources')
    assert resp.status_code == 200, resp.json

    source_cfg_keys = {'source_type', 'title', 'connection_id', 'parameters'}
    for source_cfg in resp.json['sources']:
        source_cfg_clean = {key: val for key, val in source_cfg.items() if key in source_cfg_keys}
        if source_cfg_clean['title'] in ['telephony_call', 'crm_lead_uf', 'crm_deal_uf',
                                         'crm_company_uf', 'crm_contact_uf'] \
                or source_cfg_clean['title'].startswith("crm_dynamic_items_"):
            continue  # different token

        ds = Dataset()
        ds.sources['source_1'] = ds.source(**source_cfg_clean)
        ds.source_avatars['avatar_1'] = ds.sources['source_1'].avatar()

        ds = api_v1.apply_updates(dataset=ds).dataset
        ds = api_v1.save_dataset(dataset=ds, preview=False).dataset
        ds_id = ds.id

        preview_resp = data_api_v1.get_response_for_dataset_preview(
            dataset_id=ds_id,
        )
        assert preview_resp.status_code == 200, preview_resp.json

        client.delete('/api/v1/datasets/{}'.format(ds_id))


def test_uf_sources(client, api_v1, data_api_v1, bitrix_uf_conn_id):
    conn_id = bitrix_uf_conn_id

    resp = client.get(f'/api/v1/connections/{conn_id}/info/sources')
    assert resp.status_code == 200, resp.json

    source_cfg_keys = {'source_type', 'title', 'connection_id', 'parameters'}
    for source_cfg in resp.json['sources']:
        source_cfg_clean = {key: val for key, val in source_cfg.items() if key in source_cfg_keys}
        if source_cfg_clean['title'] not in ['crm_lead_uf', 'crm_deal_uf', 'crm_company_uf', 'crm_contact_uf']:
            continue  # no uf columns

        ds = Dataset()
        ds.sources['source_1'] = ds.source(**source_cfg_clean)
        ds.source_avatars['avatar_1'] = ds.sources['source_1'].avatar()

        ds = api_v1.apply_updates(dataset=ds).dataset
        ds = api_v1.save_dataset(dataset=ds, preview=False).dataset
        ds_id = ds.id

        result_schema = get_result_schema(client, ds_id)
        columns = [col['guid'] for col in result_schema]
        assert any([column.startswith('uf_') for column in columns])

        preview_resp = data_api_v1.get_response_for_dataset_preview(
            dataset_id=ds_id,
        )
        assert preview_resp.status_code == 200, preview_resp.json

        client.delete('/api/v1/datasets/{}'.format(ds_id))


def test_smart_tables_sources(client, api_v1, data_api_v1, bitrix_smart_tables_conn_id):
    conn_id = bitrix_smart_tables_conn_id

    resp = client.get(f'/api/v1/connections/{conn_id}/info/sources')
    assert resp.status_code == 200, resp.json

    smart_tables_cnt = 0
    source_cfg_keys = {'source_type', 'title', 'connection_id', 'parameters'}
    for source_cfg in resp.json['sources']:
        source_cfg_clean = {key: val for key, val in source_cfg.items() if key in source_cfg_keys}
        if not source_cfg_clean['title'].startswith("crm_dynamic_items_"):
            continue  # different token

        smart_tables_cnt += 1
        ds = Dataset()
        ds.sources['source_1'] = ds.source(**source_cfg_clean)
        ds.source_avatars['avatar_1'] = ds.sources['source_1'].avatar()

        ds = api_v1.apply_updates(dataset=ds).dataset
        ds = api_v1.save_dataset(dataset=ds, preview=False).dataset
        ds_id = ds.id

        preview_resp = data_api_v1.get_response_for_dataset_preview(
            dataset_id=ds_id,
        )
        assert preview_resp.status_code == 200, preview_resp.json

        client.delete('/api/v1/datasets/{}'.format(ds_id))
    assert smart_tables_cnt >= 2
