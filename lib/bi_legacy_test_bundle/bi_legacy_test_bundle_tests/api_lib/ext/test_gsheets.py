from __future__ import annotations

from contextlib import contextmanager
import json
from typing import Generator

import pytest

from bi_legacy_test_bundle_tests.api_lib.utils import (
    get_random_str,
    get_result_schema,
)
from dl_api_client.dsmaker.primitives import Dataset

import bi_connector_gsheets.core.gozora
from bi_connector_gsheets.core.us_connection import (
    GSheetsConnection,
    GSheetsConnectOptions,
)

GSHEETS_EXAMPLE_URL = (
    "https://docs.google.com/spreadsheets/d/1zRPTxxLOQ08n0_MReIBbouAbPFw6pJ4ibNSVdFkZ3gs/edit?usp=sharing"
)
GSHEETS_CONN_PARAMS_BASE = dict(
    type="gsheets",
    url=GSHEETS_EXAMPLE_URL,
)


@contextmanager
def gozora_enabled(tvm_secret_reader) -> Generator[None, None, None]:
    old_get_conn_options = GSheetsConnection.get_conn_options
    old_tvm_info_and_secret = bi_connector_gsheets.core.gozora.TvmCliSingletonGoZora.tvm_info_and_secret

    def new_get_conn_options(self) -> GSheetsConnectOptions:
        conn_options = old_get_conn_options(self)
        return conn_options

    try:
        GSheetsConnection.get_conn_options = new_get_conn_options
        bi_connector_gsheets.core.gozora.TvmCliSingletonGoZora.tvm_info_and_secret = tvm_secret_reader.get_tvm_info()
        yield
    finally:
        GSheetsConnection.get_conn_options = old_get_conn_options
        bi_connector_gsheets.core.gozora.TvmCliSingletonGoZora.tvm_info_and_secret = old_tvm_info_and_secret


@pytest.fixture
def gsheets_conn_params():
    return dict(
        GSHEETS_CONN_PARAMS_BASE,
        name="gsheets_test_{}".format(get_random_str()),
    )


@pytest.fixture
def gsheets_conn_id(app, client, request, gsheets_conn_params):
    conn_params = gsheets_conn_params
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


def test_gsheets_cache_ttl_sec_override(client, api_v1, default_sync_usm, gsheets_conn_id):
    conn_id = gsheets_conn_id
    conn = client.get("/api/v1/connections/{}".format(conn_id)).json
    assert conn["cache_ttl_sec"] is None

    cache_ttl_override = 100500
    conn_data = dict(
        # url=conn['url'],
    )
    conn_data["cache_ttl_sec"] = cache_ttl_override
    resp = client.put(
        "/api/v1/connections/{}".format(conn_id), data=json.dumps(conn_data), content_type="application/json"
    )
    assert resp.status_code == 200, resp.json

    resp = client.get(f"/api/v1/connections/{conn_id}")
    assert resp.status_code == 200
    resp_data = resp.json
    assert resp_data["cache_ttl_sec"] == cache_ttl_override, resp_data


def test_gsheets_conn_test(client, api_v1, default_sync_usm, gsheets_conn_params, tvm_info):
    conn_params = gsheets_conn_params
    resp = client.post(
        "/api/v1/connections/test_connection_params",
        data=json.dumps(conn_params),
        content_type="application/json",
    )
    assert resp.status_code == 200, resp.json


def test_gsheets_conn_test_with_gozora(client, api_v1, gsheets_conn_params, tvm_secret_reader):
    conn_params = gsheets_conn_params
    with gozora_enabled(tvm_secret_reader):
        resp = client.post(
            "/api/v1/connections/test_connection_params",
            data=json.dumps(conn_params),
            content_type="application/json",
        )
    assert resp.status_code == 200, resp.json


def test_gsheets_conn_test_error(client, gsheets_conn_params):
    conn_params = gsheets_conn_params
    resp = client.post(
        "/api/v1/connections/test_connection_params",
        data=json.dumps(
            dict(
                conn_params,
                url="zxc" + conn_params["url"],
            )
        ),
        content_type="application/json",
    )
    assert resp.status_code == 400, resp.json


@pytest.fixture
def gsheets_dataset(client, api_v1, request, gsheets_conn_id):
    conn_id = gsheets_conn_id

    resp = client.get(f"/api/v1/connections/{conn_id}/info/sources")
    assert resp.status_code == 200, resp.json
    source_cfg = resp.json["sources"][0]
    source_cfg_keys = {"source_type", "title", "connection_id", "parameters"}
    source_cfg_clean = {key: val for key, val in source_cfg.items() if key in source_cfg_keys}

    ds = Dataset()
    ds.sources["source_1"] = ds.source(**source_cfg_clean)
    ds.source_avatars["avatar_1"] = ds.sources["source_1"].avatar()

    # TODO: preview request

    ds = api_v1.apply_updates(dataset=ds).dataset
    ds = api_v1.save_dataset(dataset=ds, preview=False).dataset

    def teardown(ds_id=ds.id):
        client.delete("/api/v1/datasets/{}".format(ds_id))

    request.addfinalizer(teardown)

    return ds


def test_gsheets_result(client, data_api_v1, gsheets_dataset):
    ds_id = gsheets_dataset.id
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
