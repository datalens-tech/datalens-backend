from __future__ import annotations

import json

import pytest

from bi_cloud_integration.yc_ts_client import get_yc_service_token_sync
from bi_legacy_test_bundle_tests.api_lib.utils import (
    get_random_str,
    get_result_schema,
)
from dl_api_client.dsmaker.primitives import Dataset

from bi_connector_yql.core.yq.constants import SOURCE_TYPE_YQ_SUBSELECT


@pytest.fixture
def yq_iam_integrated_mock(env_param_getter, iam_services_mock, ext_sys_helpers_per_test):
    """Make sure the IAM mock will return a working IAM token for the integrated-tests user"""
    key_data_s = env_param_getter.get_str_value("YQ_CLOUD_PREPROD_SVCACC_KEY_DATA")
    key_data = json.loads(key_data_s)
    iam_token = get_yc_service_token_sync(
        key_data=key_data_s,
        yc_ts_endpoint=ext_sys_helpers_per_test.ext_sys_requisites.YC_TS_ENDPOINT,
        timeout=60,
    )

    iam_data = iam_services_mock.data_holder
    user = iam_data.User(
        key_data["service_account_id"],
        iam_tokens=[iam_token],
        is_service_account=True,
        service_account_folder_id="aoevv1b69su5144mlro3",
    )
    iam_data.add_users([user])

    return iam_services_mock


@pytest.fixture
def yq_conn_params(env_param_getter, yq_iam_integrated_mock):
    return dict(
        type="yq",
        name="yq_test_{}".format(get_random_str()),
        password=env_param_getter.get_str_value("YQ_CLOUD_PREPROD_SVCACC_KEY_DATA"),
        raw_sql_level="dashsql",
    )


@pytest.fixture
def yq_conn_id(app, client, request, yq_conn_params):
    conn_params = yq_conn_params
    resp = client.post(
        "/api/v1/connections",
        data=json.dumps(conn_params),
        content_type="application/json",
    )
    assert resp.status_code == 200, resp.json
    conn_id = resp.json["id"]

    def teardown():
        client.delete("/api/v1/connections/{}".format(conn_id))

    request.addfinalizer(teardown)

    return conn_id


def test_yq_cache_ttl_sec_override(client, api_v1, default_sync_usm, yq_conn_id):
    conn_id = yq_conn_id
    conn_data = client.get("/api/v1/connections/{}".format(conn_id)).json
    assert conn_data["cache_ttl_sec"] is None

    cache_ttl_override = 100500
    req_data = dict(cache_ttl_sec=cache_ttl_override)
    resp = client.put(
        "/api/v1/connections/{}".format(conn_id), data=json.dumps(req_data), content_type="application/json"
    )
    assert resp.status_code == 200, resp.json

    resp = client.get(f"/api/v1/connections/{conn_id}")
    assert resp.status_code == 200
    resp_data = resp.json
    assert resp_data["cache_ttl_sec"] == cache_ttl_override, resp_data


def test_yq_conn_test(client, api_v1, default_sync_usm, yq_conn_params):
    conn_params = yq_conn_params
    resp = client.post(
        "/api/v1/connections/test_connection_params",
        data=json.dumps(conn_params),
        content_type="application/json",
    )
    assert resp.status_code == 200, resp.json


@pytest.fixture
def yq_dataset(client, api_v1, request, yq_conn_id):
    conn_id = yq_conn_id

    ds = Dataset()
    ds.sources["source_1"] = ds.source(
        source_type=SOURCE_TYPE_YQ_SUBSELECT,
        connection_id=conn_id,
        parameters=dict(
            subsql="select 1/3",
        ),
    )
    ds.source_avatars["avatar_1"] = ds.sources["source_1"].avatar()

    # TODO: preview request

    ds = api_v1.apply_updates(dataset=ds).dataset
    ds = api_v1.save_dataset(dataset=ds, preview=False).dataset

    def teardown(ds_id=ds.id):
        client.delete("/api/v1/datasets/{}".format(ds_id))

    request.addfinalizer(teardown)

    return ds


def test_yq_result(client, data_api_v1, yq_dataset, yq_iam_integrated_mock):
    ds_id = yq_dataset.id
    result_schema = get_result_schema(client, ds_id)
    columns = [col["guid"] for col in result_schema]
    response = data_api_v1.get_response_for_dataset_result(
        dataset_id=ds_id,
        raw_body=dict(columns=columns, limit=3),  # implies `group by` for all columns.
    )
    assert response.status_code == 200
    rd = response.json
    types = rd["result"]["data"]["Type"][1][1]
    assert len(types) == len(columns)


def test_yq_sa_authorization_mocked(app, client, request, iam_services_mock, remote_async_adapter_mock):
    service_account_ids = [
        "fake_test_service_account_id",
        "fake_second_test_service_account_id",
        "fake_third_test_service_account_id",
    ]
    conn_params = dict(
        type="yq",
        name="yq_test_{}".format(get_random_str()),
        folder_id="fake_test_folder_id",
        service_account_id=service_account_ids[0],
        raw_sql_level="subselect",
    )
    iam_data = iam_services_mock.data_holder
    user = iam_data.make_new_user(
        (iam_data.Resource.service_account(service_account_ids[0]), "iam.serviceAccounts.use"),
        (iam_data.Resource.service_account(service_account_ids[2]), "iam.serviceAccounts.use"),
    )
    headers = {"X-YaCloud-SubjectToken": user.iam_tokens[0]}

    resp = client.post(
        "/api/v1/connections/test_connection_params",
        data=json.dumps(conn_params),
        content_type="application/json",
        headers=headers,
    )
    assert resp.status_code == 200, resp.json
    # TODO: check that auth was checked

    resp = client.post(
        "/api/v1/connections",
        data=json.dumps(conn_params),
        content_type="application/json",
        headers=headers,
    )
    assert resp.status_code == 200, resp.json
    conn_id = resp.json["id"]
    # TODO: check that auth was checked

    resp = client.put(
        "/api/v1/connections/{}".format(conn_id),
        data=json.dumps(dict(conn_params, raw_sql_level="dashsql")),
        content_type="application/json",
        headers=headers,
    )
    assert resp.status_code == 200, resp.json
    # TODO: check that auth was *NOT* checked

    resp = client.put(
        "/api/v1/connections/{}".format(conn_id),
        data=json.dumps(dict(conn_params, service_account_id=service_account_ids[1])),
        content_type="application/json",
        headers=headers,
    )
    assert resp.status_code == 403, resp.json
    # TODO: check that auth was checked in the mockup.

    resp = client.put(
        "/api/v1/connections/{}".format(conn_id),
        data=json.dumps(dict(conn_params, service_account_id=service_account_ids[2])),
        content_type="application/json",
        headers=headers,
    )
    assert resp.status_code == 200, resp.json
