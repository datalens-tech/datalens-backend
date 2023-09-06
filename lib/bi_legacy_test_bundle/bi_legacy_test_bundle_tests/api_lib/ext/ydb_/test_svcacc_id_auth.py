from __future__ import annotations

import json

from bi_legacy_test_bundle_tests.api_lib.utils import get_random_str


def test_ydb_sa_authorization_mocked(app, client, request, iam_services_mock, remote_async_adapter_mock):
    service_account_ids = ["fake_test_service_account_id", "fake_second_test_service_account_id", "fake_third_test_service_account_id"]
    conn_params = dict(
        type="ydb",
        name="ydb_test_{}".format(get_random_str()),

        host="ydb-datalens-tests-fake.yandex.net", port=2135, token="",
        db_name="/ru/yql/test/ydb-datalens-tests-fake",

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
