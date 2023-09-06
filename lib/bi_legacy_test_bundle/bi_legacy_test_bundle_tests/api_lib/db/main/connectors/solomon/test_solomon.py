from __future__ import annotations

import json
from bi_core_testing.flask_utils import FlaskTestClient


def test_create_solomon_connection(client: FlaskTestClient, solomon_connection_params):
    resp = client.post(
        '/api/v1/connections',
        data=json.dumps(solomon_connection_params),
        content_type='application/json'
    )
    assert resp.status_code == 200
    conn_id = resp.json['id']

    resp = client.get(f'/api/v1/connections/{conn_id}')
    assert resp.status_code == 200
    assert resp.json['db_type'] == 'solomon'
