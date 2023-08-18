from __future__ import annotations

import json

import pytest


class BaseChytConnectionTest:
    def test_connection_tester_errors(self, client, connection_params, headers):
        params = dict(connection_params)
        params['alias'] = '*definitely_nonexistent_test_clique'
        resp = client.post(
            '/api/v1/connections/test_connection_params',
            data=json.dumps(params),
            content_type='application/json',
            headers=headers,
        )
        assert resp.status_code == 400, resp
        rd = resp.json
        assert rd['details']['db_message'].startswith('Invalid clique'), rd

    async def test_dashsql_result(self, async_api_local_env_low_level_client, connection_id, headers):
        data_api_aio = async_api_local_env_low_level_client
        req_data = {
            'sql_query': 'select 1',
        }
        resp = await data_api_aio.request(
            'post', f'/api/v1/connections/{connection_id}/dashsql',
            json=req_data,
            headers=headers)
        resp_data = await resp.json()
        assert resp.status == 200
        assert resp_data[-1]['event'] == 'footer'

    def test_clique_alias_validator(self, client, connection_params, headers):
        params = dict(connection_params)
        params['alias'] = '*ch_public'
        resp = client.post(
            '/api/v1/connections/test_connection_params',
            data=json.dumps(params),
            content_type='application/json',
            headers=headers,
        )
        assert resp.status_code == 400, resp


class TestChytConnection(BaseChytConnectionTest):
    def test_connection_tester(self, client, connection_params):
        r = client.post(
            '/api/v1/connections/test_connection_params',
            data=json.dumps(connection_params),
            content_type='application/json',
        )
        assert r.status_code == 200

        r = client.post(
            '/api/v1/connections/test_connection_params',
            data=json.dumps({**connection_params, 'token': 'helloworld'}),
            content_type='application/json',
        )
        assert r.status_code == 400

    def test_create_get_connection(self, client, connection_params):
        resp = client.post(
            '/api/v1/connections',
            data=json.dumps(connection_params),
            content_type='application/json',
        )
        assert resp.status_code == 200
        conn_id = resp.json['id']

        resp = client.get(f'/api/v1/connections/{conn_id}')
        assert resp.status_code == 200
        assert resp.json['db_type'] == 'ch_over_yt'
        assert resp.json.get('token') is None

    @pytest.fixture(scope='function')
    def connection_params(self, public_ch_over_yt_connection_params):
        return public_ch_over_yt_connection_params

    @pytest.fixture(scope='function')
    def headers(self):
        return {}

    @pytest.fixture(scope='function')
    def connection_id(self, public_ch_over_yt_connection_id):
        return public_ch_over_yt_connection_id


class TestChytUserAuthConnection(BaseChytConnectionTest):
    def test_connection_tester(
        self, client, connection_params, public_ch_over_yt_user_auth_headers
    ):
        r = client.post(
            '/api/v1/connections/test_connection_params',
            data=json.dumps(connection_params),
            content_type='application/json',
            headers=public_ch_over_yt_user_auth_headers,
        )
        assert r.status_code == 200

        r = client.post(
            '/api/v1/connections/test_connection_params',
            data=json.dumps({**connection_params}),
            content_type='application/json',
        )
        assert r.status_code == 400

    @pytest.fixture(scope='function')
    def connection_params(self, public_ch_over_yt_user_auth_connection_params):
        return public_ch_over_yt_user_auth_connection_params

    @pytest.fixture(scope='function')
    def headers(self, public_ch_over_yt_user_auth_headers):
        return public_ch_over_yt_user_auth_headers

    @pytest.fixture(scope='function')
    def connection_id(self, public_ch_over_yt_user_auth_connection_id):
        return public_ch_over_yt_user_auth_connection_id
