from __future__ import annotations

import json
import random

from bi_connector_clickhouse.core.us_connection import ConnectionClickhouse
from bi_core.us_manager.us_manager_sync import SyncUSManager
from bi_core import exc as common_exc

from bi_api_lib_tests.conftest import ch_connection_params
from bi_testing.utils import guids_from_titles
from bi_api_lib_tests.utils import get_result_schema
from bi_api_lib_tests.conftest import _make_dataset


def test_get_connection(client, connection_id):
    conn = client.get('/api/v1/connections/{}'.format(connection_id)).json
    assert 'db_type' in conn
    assert 'host' in conn
    assert 'id' in conn
    assert 'password' not in conn
    assert conn['raw_sql_level'] in ('off', 'subselect', 'dashsql'), "should not be 'unknown'"


def test_create_edit_and_delete_connection(client, default_sync_usm_per_test: SyncUSManager):
    usm = default_sync_usm_per_test
    conn_name = 'test__{}'.format(random.randint(0, 10000000))
    conn_data = {
        'host': 'localhost',
        'port': 1337,
        'username': 'root',
        'mdb_cluster_id': 'some_cluster_id',
        'mdb_folder_id': 'some_folder_id',
    }
    conn_id = client.post(
        '/api/v1/connections',
        data=json.dumps({
            'type': 'clickhouse',
            'name': conn_name,
            **conn_data,
            'password': 'qwerty',
            'secure': 'on',
        }),
        content_type='application/json'
    ).json['id']

    initial_saved_password = 'qwerty'

    conn_data['username'] = 'user123'
    r = client.put(
        '/api/v1/connections/{}'.format(conn_id),
        data=json.dumps(conn_data),
        content_type='application/json'
    )
    assert r.status_code == 200
    conn = client.get('/api/v1/connections/{}'.format(conn_id)).json
    assert conn['username'] == 'user123'
    assert 'password' not in conn
    assert conn['secure'] == 'on'
    assert conn['mdb_cluster_id'] == conn_data['mdb_cluster_id']
    assert conn['mdb_folder_id'] == conn_data['mdb_folder_id']

    saved_password_after_put = usm.get_by_id(conn_id, expected_type=ConnectionClickhouse).password
    assert saved_password_after_put == initial_saved_password
    assert conn['cache_ttl_sec'] is None

    conn_data['mdb_cluster_id'] = 'new_db_cluster_id'
    conn_data['mdb_folder_id'] = 'some_folder_id'
    r = client.put(
        '/api/v1/connections/{}'.format(conn_id),
        data=json.dumps(conn_data),
        content_type='application/json'
    )
    assert r.status_code == 200

    r = client.get(f'/api/v1/connections/{conn_id}')
    assert r.status_code == 200
    assert conn_data['mdb_cluster_id'] == r.json['mdb_cluster_id']
    assert conn_data['mdb_folder_id'] == r.json['mdb_folder_id']

    # Cache TTL override
    cache_ttl_override = 100500

    conn_data['cache_ttl_sec'] = cache_ttl_override
    r = client.put(
        '/api/v1/connections/{}'.format(conn_id),
        data=json.dumps(conn_data),
        content_type='application/json'
    )
    assert r.status_code == 200, r.json

    r = client.get(f'/api/v1/connections/{conn_id}')
    assert r.status_code == 200
    assert r.json['cache_ttl_sec'] == cache_ttl_override

    # Cache TTL remove override
    conn_data['cache_ttl_sec'] = None
    r = client.put(
        '/api/v1/connections/{}'.format(conn_id),
        data=json.dumps(conn_data),
        content_type='application/json'
    )
    assert r.status_code == 200

    r = client.get(f'/api/v1/connections/{conn_id}')
    assert r.status_code == 200
    assert r.json['cache_ttl_sec'] is None

    # Delete
    client.delete('/api/v1/connections/{}'.format(conn_id))
    assert client.get('/api/v1/connections/{}'.format(conn_id)).status_code == 404


def test_create_edit_conn_with_data_export_forbidden_flag(client, default_sync_usm_per_test: SyncUSManager):
    conn_name = 'test__{}'.format(random.randint(0, 10000000))
    conn_data = {
        'host': 'localhost',
        'port': 1337,
        'username': 'root',
        'mdb_cluster_id': 'some_cluster_id',
        'mdb_folder_id': 'some_folder_id',
    }
    r = client.post(
        '/api/v1/connections',
        data=json.dumps({
            'type': 'clickhouse',
            'name': conn_name,
            **conn_data,
            'password': 'qwerty',
            'secure': 'on',
        }),
        content_type='application/json'
    )

    conn_id = r.json['id']

    assert r.status_code == 200
    conn = client.get('/api/v1/connections/{}'.format(conn_id)).json
    assert conn['data_export_forbidden'] == 'off'

    conn_data['data_export_forbidden'] = 'on'
    r = client.put(
        '/api/v1/connections/{}'.format(conn_id),
        data=json.dumps(conn_data),
        content_type='application/json'
    )
    assert r.status_code == 200

    r = client.get(f'/api/v1/connections/{conn_id}')
    conn = r.json
    assert r.status_code == 200
    assert conn['data_export_forbidden'] == 'on'


def test_get_connection_metadata_sources(client, connection_id):
    response = client.get(f'/api/v1/connections/{connection_id}/info/metadata_sources')

    assert response.status_code == 200

    resp_data = response.json
    source_templates = resp_data['sources']
    if source_templates:  # generally empty for SQLs
        assert source_templates[0]['connection_id'] == connection_id
        assert source_templates[0]['source_type'] == 'CH_TABLE'

    freeform_sources = resp_data['freeform_sources']
    assert freeform_sources
    subselects = [item for item in freeform_sources if item['source_type'].endswith('_SUBSELECT')]
    assert len(subselects) == 1
    subselect = subselects[0]
    assert subselect['disabled'] is True


def test_get_connection_metadata_sources_no_permissions(client, connection_id, mock_permissions_for_us_entries):
    with mock_permissions_for_us_entries(read=False):
        response = client.get(f'/api/v1/connections/{connection_id}/info/metadata_sources')

    assert response.status_code == 200

    resp_data = response.json
    source_templates = resp_data['sources']
    assert source_templates == []

    freeform_sources = resp_data['freeform_sources']
    assert freeform_sources
    subselects = [item for item in freeform_sources if item['source_type'].endswith('_SUBSELECT')]
    assert len(subselects) == 1
    subselect = subselects[0]
    assert subselect['disabled'] is True


def test_get_connection_source_templates(client, connection_id, dataset_id):
    # Note: `dataset_id` to ensure there are sources.
    response = client.get(f'/api/v1/connections/{connection_id}/info/sources')
    assert response.status_code == 200

    resp_data = response.json
    source_templates = resp_data['sources']
    assert source_templates, resp_data
    some_source = source_templates[0]
    assert some_source['connection_id'] == connection_id, some_source
    assert some_source['source_type'] == 'CH_TABLE', some_source

    # TODO: phase out in favor of `info/metadata_sources`
    freeform_sources = resp_data['freeform_sources']
    assert freeform_sources, resp_data
    subselects = [item for item in freeform_sources if item['source_type'].endswith('_SUBSELECT')]
    assert len(subselects) == 1, freeform_sources
    subselect = subselects[0]
    assert subselect['disabled'] is True, subselect

    # check filtering
    title_part = some_source['title'][-4:]
    response = client.get(
        f'/api/v1/connections/{connection_id}/info/sources?limit=5&search_text={title_part}',
    )
    resp_data = response.json
    srcs = resp_data['sources']
    assert srcs, resp_data
    assert some_source in srcs

    # check limiting
    response = client.get(
        f'/api/v1/connections/{connection_id}/info/sources?limit=2',
    )
    resp_data = response.json
    srcs = resp_data['sources']
    assert len(srcs) in (1, 2), srcs


def test_get_connection_source_templates_no_permissions(
    client,
    connection_id,
    dataset_id,
    mock_permissions_for_us_entries
):
    # Note: `dataset_id` to ensure there are sources.
    with mock_permissions_for_us_entries(read=False):
        response = client.get(f'/api/v1/connections/{connection_id}/info/sources')

    assert response.status_code == 200

    resp_data = response.json
    source_templates = resp_data['sources']
    assert source_templates == []

    # TODO: phase out in favor of `info/metadata_sources`
    freeform_sources = resp_data['freeform_sources']
    assert freeform_sources, resp_data
    subselects = [item for item in freeform_sources if item['source_type'].endswith('_SUBSELECT')]
    assert len(subselects) == 1, freeform_sources
    subselect = subselects[0]
    assert subselect['disabled'] is True, subselect


def test_get_connection_source_schema(client, pg_subselectable_connection_id, dataset_id):
    # Note: `dataset_id` to ensure there are sources.
    connection_id = pg_subselectable_connection_id
    response = client.get(f'/api/v1/connections/{connection_id}/info/sources')
    resp_data = response.json

    source_templates = resp_data['sources']

    for source in source_templates:
        source['id'] = 'some_id'
        response = client.post(
            f'/api/v1/connections/{connection_id}/info/source/schema',
            data=json.dumps({'source': source}),
            content_type='application/json',
        )
        assert response.status_code == 200
        resp_data = response.json
        assert resp_data['raw_schema']


def test_connection_tester_clickhouse(client, app):
    data = ch_connection_params(app).copy()
    data.pop('name', None)
    r = client.post(
        '/api/v1/connections/test_connection_params',
        data=json.dumps(data),
        content_type='application/json',
    )
    assert r.status_code == 200

    r = client.post(
        '/api/v1/connections/test_connection_params',
        data=json.dumps({**data, 'password': 'helloworld'}),
        content_type='application/json',
    )
    assert r.json['message'] == common_exc.DbAuthenticationFailed.default_message
    assert r.status_code == 400


def test_connection_tester_existing_ch_connection(client, connection_id):
    r = client.post(
        '/api/v1/connections/test_connection/{}'.format(connection_id),
        content_type='application/json',
        data=json.dumps({'username': 'johndoe'})
    )
    assert r.status_code == 400

    r = client.post(
        '/api/v1/connections/test_connection/{}'.format(connection_id),
        content_type='application/json',
        data=json.dumps({}),
    )
    assert r.status_code == 200

    r = client.post(
        '/api/v1/connections/test_connection/{}'.format(connection_id),
        content_type='application/json',
        data=json.dumps({'password': 'helloworld'})
    )
    assert r.status_code == 400


def test_create_greenplum_conn(client, pg_connection_params):
    params = pg_connection_params
    params.update(type='greenplum', cache_ttl_sec=None)
    resp = client.post(
        '/api/v1/connections',
        data=json.dumps(params),
        content_type='application/json'
    )
    assert resp.status_code == 200
    conn_id = resp.json['id']

    resp = client.get(f'/api/v1/connections/{conn_id}')
    assert resp.status_code == 200
    conn = resp.json
    assert conn['db_type'] == 'greenplum'
    assert conn['cache_ttl_sec'] is None

    resp = client.post(
        f'/api/v1/connections/test_connection/{conn_id}',
        data=json.dumps(params),
        content_type='application/json'
    )
    assert resp.status_code == 200

    params.update(port=999)
    resp = client.post(
        f'/api/v1/connections/test_connection/{conn_id}',
        data=json.dumps(params),
        content_type='application/json'
    )
    assert resp.status_code == 400


def test_test_connection_params_greenplum(client, pg_connection_params):
    params = pg_connection_params
    params.update(type='greenplum', cache_ttl_sec=None)
    resp = client.post(
        '/api/v1/connections/test_connection_params',
        data=json.dumps(params),
        content_type='application/json'
    )
    assert resp.status_code == 200

    params.update(port=999)
    resp = client.post(
        '/api/v1/connections/test_connection_params',
        data=json.dumps(params),
        content_type='application/json'
    )
    assert resp.status_code == 400


def test_delete_connection_with_unknown_type(client, connection_id, default_sync_usm_per_test):
    usm = default_sync_usm_per_test
    us_client = usm._us_client
    conn_entry_dict = us_client.get_entry(connection_id)

    new_type = 'helloworld'

    us_client._request(
        us_client.RequestData(
            method='post',
            relative_url='/entries/{}'.format(connection_id),
            params=None,
            json={
                'type': new_type,
                'data': conn_entry_dict['data'],
                'unversionedData': conn_entry_dict['unversionedData'],
                'meta': conn_entry_dict['meta'],
                'mode': 'publish',
                'links': conn_entry_dict['links'],
            }
        )
    )

    api_resp = client.get('/api/v1/connections/{}'.format(connection_id))
    assert api_resp.status_code == 200
    assert api_resp.json['db_type'] == 'unknown'
    api_resp = client.delete('/api/v1/connections/{}'.format(connection_id))
    assert api_resp.status_code == 200
    api_resp = client.get('/api/v1/connections/{}'.format(connection_id))
    assert api_resp.status_code == 404


def test_update_data_export_forbidden_flag_and_get_dataset(client, data_api_v1, connection_id, request):
    dataset_id = _make_dataset(client, connection_id, request)

    result_schema = get_result_schema(client, dataset_id)
    req_data = {
        'columns': guids_from_titles(result_schema, ['Discount', 'City']),
    }

    r = data_api_v1.get_response_for_dataset_result(dataset_id=dataset_id, raw_body=req_data)
    assert not r.json['result']['data_export_forbidden']
    client.put(
        '/api/v1/connections/{}'.format(connection_id),
        data=json.dumps({
            'data_export_forbidden': 'on',
        }),
        content_type='application/json'
    )
    r = data_api_v1.get_response_for_dataset_result(dataset_id=dataset_id, raw_body=req_data)
    assert r.json['result']['data_export_forbidden']
