import json
import uuid

from bi_api_lib_testing.connector.connection_suite import DefaultConnectorConnectionTestSuite
from bi_connector_snowflake_tests.ext.api.base import SnowFlakeConnectionTestBase


class TestSnowFlakeConnection(SnowFlakeConnectionTestBase, DefaultConnectorConnectionTestSuite):
    def test_connection_parameters_tester(self, client, connection_params):
        params = {
            "type": "snowflake",
            "name": uuid.uuid4().hex,
        }
        params.update(connection_params)
        r = client.post(
            '/api/v1/connections/test_connection_params',
            data=json.dumps(params),
            content_type='application/json',
        )
        assert r.status_code == 200

    def test_create_connection_get_tester(self, client, connection_params):
        params = {
            "type": "snowflake",
            "name": uuid.uuid4().hex,
        }
        params.update(connection_params)
        resp = client.post(
            '/api/v1/connections',
            data=json.dumps(params),
            content_type='application/json',
        )
        assert resp.status_code == 200
        conn_id = resp.json['id']

        resp = client.get(f'/api/v1/connections/{conn_id}')
        assert resp.status_code == 200

        data = resp.json
        for k, v in connection_params.items():
            if k in ("client_secret", "refresh_token"):
                continue

            assert v == data[k]

        resp = client.post(
            f'/api/v1/connections/test_connection/{conn_id}',
            data=json.dumps({}),
            content_type='application/json',
        )
        assert resp.status_code == 200

    def test_connection_malicious_account_name(self, client, connection_params):
        bad_names = ["foo;", "https://fake", "your-site.com/oauth/token-request?"]
        params = {
            "type": "snowflake",
            "name": uuid.uuid4().hex,
        }
        params.update(connection_params)
        for bad_name in bad_names:
            params["account_name"] = bad_name
            resp = client.post(
                '/api/v1/connections/test_connection_params',
                data=json.dumps(params),
                content_type='application/json',
            )
            assert resp.status_code == 400

            resp = client.post(
                '/api/v1/connections',
                data=json.dumps(params),
                content_type='application/json',
            )
            assert resp.status_code == 400
            assert "account_name" in resp.json['message']
            assert "id" not in resp.json
