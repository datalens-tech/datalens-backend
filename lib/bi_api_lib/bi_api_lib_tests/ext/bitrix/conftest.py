import json

import pytest

from bi_api_client.dsmaker.primitives import Dataset

from bi_api_lib_tests.utils import get_random_str


@pytest.fixture(scope='session')
def bitrix_token(env_param_getter):
    return env_param_getter.get_str_value('BITRIX_TOKEN')


@pytest.fixture(scope='session')
def bitrix_uf_token(env_param_getter):
    return env_param_getter.get_str_value('BITRIX_SERBUL_TOKEN')


@pytest.fixture(scope='session')
def bitrix_smart_tables_token(env_param_getter):
    return env_param_getter.get_str_value('BITRIX_SERBUL_TOKEN')


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
