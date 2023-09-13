import json
import random

import pytest
import shortuuid

from bi_testing_ya.dlenv import DLEnv
from bi_api_client.dsmaker.primitives import Dataset

from bi_connector_clickhouse.core.clickhouse.constants import SOURCE_TYPE_CH_TABLE


@pytest.mark.parametrize("dl_env", [DLEnv.dc_testing], indirect=True)
def test_create_cp_app(dl_env, dc_rs_cp_app):
    pass


@pytest.mark.parametrize("dl_env", [DLEnv.dc_testing], indirect=True)
def test_get_connectors_summary(dl_env, dc_rs_bi_api_client, dc_rs_workbook_id):
    assert dc_rs_workbook_id
    response = dc_rs_bi_api_client.get("/api/v1/info/connectors")
    assert response.status_code == 200


@pytest.mark.parametrize("dl_env", [DLEnv.dc_testing], indirect=True)
def test_connector_tester_anon(dl_env, dc_rs_bi_api_client, dc_rs_conn_data_clickhouse, dc_rs_workbook_id):
    client = dc_rs_bi_api_client

    conn_name = 'test__{}'.format(random.randint(0, 10000000))
    conn_data = {
        'type': 'clickhouse',
        'name': conn_name,
        'workbook_id': dc_rs_workbook_id,
        **dc_rs_conn_data_clickhouse,
    }

    resp = client.post(
        "/api/v1/connections/test_connection_params",
        data=json.dumps(conn_data),
        content_type='application/json',
    )
    assert resp.status_code == 200, resp.json


@pytest.mark.parametrize("dl_env", [DLEnv.dc_testing], indirect=True)
def test_get_connection(dl_env, dc_rs_bi_api_client, dc_rs_connection_id_clickhouse, dc_rs_workbook_id):
    get_resp = dc_rs_bi_api_client.get(f'/api/v1/connections/{dc_rs_connection_id_clickhouse}')
    assert get_resp.status_code == 200, get_resp.json

    actual_workbook_id = get_resp.json["workbook_id"]
    actual_conn_name = get_resp.json["name"]
    actual_conn_us_key = get_resp.json["key"]
    assert actual_workbook_id == dc_rs_workbook_id
    assert actual_conn_us_key.endswith(f"/{actual_conn_name}")


@pytest.mark.parametrize("dl_env", [DLEnv.dc_testing], indirect=True)
def test_get_connection_postgres(dl_env, dc_rs_bi_api_client, dc_rs_connection_id_postgres, dc_rs_workbook_id):
    get_resp = dc_rs_bi_api_client.get(f'/api/v1/connections/{dc_rs_connection_id_postgres}')
    assert get_resp.status_code == 200, get_resp.json

    actual_workbook_id = get_resp.json["workbook_id"]
    actual_conn_name = get_resp.json["name"]
    actual_conn_us_key = get_resp.json["key"]
    assert actual_workbook_id == dc_rs_workbook_id
    assert actual_conn_us_key.endswith(f"/{actual_conn_name}")


@pytest.mark.parametrize("dl_env", [DLEnv.dc_testing], indirect=True)
def test_edit_connection(dl_env, dc_rs_bi_api_client, dc_rs_connection_id_clickhouse, dc_rs_workbook_id):
    patch = dict(
        host="changed.host",
        port=23445,
    )

    edit_resp = dc_rs_bi_api_client.put(f'/api/v1/connections/{dc_rs_connection_id_clickhouse}', json=patch)
    assert edit_resp.status_code == 200, edit_resp.json

    get_resp = dc_rs_bi_api_client.get(f'/api/v1/connections/{dc_rs_connection_id_clickhouse}')
    assert get_resp.status_code == 200, get_resp.json

    changed_fields = {
        f_name: f_val
        for f_name, f_val in get_resp.json.items()
        if f_name in patch
    }

    assert changed_fields == patch


@pytest.mark.parametrize("dl_env", [DLEnv.dc_testing], indirect=True)
def test_sources(dl_env, dc_rs_bi_api_client, dc_rs_connection_id_clickhouse):
    sources_resp = dc_rs_bi_api_client.get(f'/api/v1/connections/{dc_rs_connection_id_clickhouse}/info/sources')
    assert sources_resp.status_code == 200
    all_tables = [s['parameters']['table_name'] for s in sources_resp.json['sources'] if s['source_type'] == 'CH_TABLE']
    assert 'sample_superstore' in all_tables


@pytest.mark.parametrize("dl_env", [DLEnv.dc_testing], indirect=True)
def test_create_dataset(
        dl_env,
        dc_rs_ds_api_set,
        dc_rs_bi_api_client,
        dc_rs_connection_id_clickhouse,
        dc_rs_workbook_id,
):
    api_v1 = dc_rs_ds_api_set.control_api_v1

    expected_ds_name = f"unit tests ds {shortuuid.uuid()}"

    ds = Dataset(
        name=expected_ds_name,
    )
    ds.sources['source_1'] = ds.source(
        source_type=SOURCE_TYPE_CH_TABLE,
        connection_id=dc_rs_connection_id_clickhouse,
        parameters=dict(
            db_name='test_data',
            table_name='sample_superstore',
        )
    )
    ds.source_avatars['avatar_1'] = ds.sources['source_1'].avatar()
    ds = api_v1.apply_updates(dataset=ds).dataset
    ds_resp = api_v1.save_dataset(
        dataset=ds,
        workbook_id=dc_rs_workbook_id,
    )
    assert ds_resp.status_code == 200
    ds = ds_resp.dataset

    get_ds_resp = dc_rs_bi_api_client.get(f"/api/v1/datasets/{ds.id}/versions/draft")
    assert get_ds_resp.status_code == 200, get_ds_resp.json
    actual_ds_key = get_ds_resp.json['key']
    assert actual_ds_key.endswith(f"/{expected_ds_name}") and len(actual_ds_key.split("/")) == 2

    actual_workbook_id = get_ds_resp.json['workbook_id']
    assert actual_workbook_id == dc_rs_workbook_id
