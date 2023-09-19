from __future__ import annotations

import logging

from multidict import CIMultiDict
import pytest


LOGGER = logging.getLogger(__name__)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "dashsql_params_types",
    [
        "string",
        "datetime",
    ],
)
async def test_solomon_dashsql_result(
    async_api_local_env_low_level_client_with_bb,
    solomon_subselectable_connection_id,
    int_cookie,
    dashsql_params_types,
):
    data_api_aio = async_api_local_env_low_level_client_with_bb
    conn_id = solomon_subselectable_connection_id

    resp = await data_api_aio.request(
        "post",
        f"/api/v1/connections/{conn_id}/dashsql",
        json={
            "sql_query": '{project="datalens", cluster="preprod_alerts", service="alerts", host="alerts"}',
            "params": {
                "project_id": {"type_name": "string", "value": "datalens"},
                "from": {"type_name": dashsql_params_types, "value": "2021-11-15T00:00:00+00:00"},
                "to": {"type_name": dashsql_params_types, "value": "2021-11-16T00:00:00+00:00"},
            },
        },
        headers=CIMultiDict(
            {
                "Cookie": int_cookie,
                "Host": "back.datalens.yandex-team.ru",
                "X-Forwarded-For": "2a02:6b8:0:51e:fd68:81c0:5d34:4531, 2a02:6b8:c12:422b:0:41c8:85ec:0",
            }
        ),
    )
    resp_data = await resp.json()
    assert resp.status == 200, resp_data


@pytest.mark.asyncio
async def test_solomon_dashsql_result_with_connector_specific_params(
    async_api_local_env_low_level_client_with_bb,
    solomon_subselectable_connection_id,
    int_cookie,
):
    data_api_aio = async_api_local_env_low_level_client_with_bb
    conn_id = solomon_subselectable_connection_id

    resp = await data_api_aio.request(
        "post",
        f"/api/v1/connections/{conn_id}/dashsql",
        json={
            "sql_query": '{project="datalens", cluster="preprod_alerts", service="alerts", host="alerts"}',
            "connector_specific_params": {
                "project_id": "datalens",
                "from": "2021-11-15T00:00:00+00:00",
                "to": "2021-11-16T00:00:00+00:00",
            },
        },
        headers=CIMultiDict(
            {
                "Cookie": int_cookie,
                "Host": "back.datalens.yandex-team.ru",
                "X-Forwarded-For": "2a02:6b8:0:51e:fd68:81c0:5d34:4531, 2a02:6b8:c12:422b:0:41c8:85ec:0",
            }
        ),
    )
    resp_data = await resp.json()
    assert resp.status == 200, resp_data


@pytest.mark.asyncio
async def test_solomon_dashsql_result_with_alias(
    async_api_local_env_low_level_client_with_bb,
    solomon_subselectable_connection_id,
    int_cookie,
):
    data_api_aio = async_api_local_env_low_level_client_with_bb
    conn_id = solomon_subselectable_connection_id

    resp = await data_api_aio.request(
        "post",
        f"/api/v1/connections/{conn_id}/dashsql",
        json={
            "sql_query": 'alias(constant_line(100), "100%")',
            "connector_specific_params": {
                "project_id": "datalens",
                "from": "2021-11-15T00:00:00+00:00",
                "to": "2021-11-16T00:00:00+00:00",
            },
        },
        headers=CIMultiDict(
            {
                "Cookie": int_cookie,
                "Host": "back.datalens.yandex-team.ru",
                "X-Forwarded-For": "2a02:6b8:0:51e:fd68:81c0:5d34:4531, 2a02:6b8:c12:422b:0:41c8:85ec:0",
            }
        ),
    )
    resp_data = await resp.json()
    assert resp.status == 200, resp_data
    metadata = resp_data[0]["data"]
    assert "_alias" in metadata["names"]


@pytest.mark.asyncio
async def test_solomon_dashsql_result_with_different_schemas(
    async_api_local_env_low_level_client_with_bb,
    solomon_subselectable_connection_id,
    int_cookie,
):
    data_api_aio = async_api_local_env_low_level_client_with_bb
    conn_id = solomon_subselectable_connection_id

    resp = await data_api_aio.request(
        "post",
        f"/api/v1/connections/{conn_id}/dashsql",
        json={
            "sql_query": (
                'alias(series_sum("code", {project="monitoring", cluster="production", service="ui", host="cluster", '
                'code!="2*", endpoint="*", sensor="http.server.requests.status", method="*"}), "{{code}}")'
            ),
            "connector_specific_params": {
                "project_id": "monitoring",
                "from": "2022-02-03T00:00:00+00:00",
                "to": "2022-02-04T00:00:00+00:00",
            },
        },
        headers=CIMultiDict(
            {
                "Cookie": int_cookie,
                "Host": "back.datalens.yandex-team.ru",
                "X-Forwarded-For": "2a02:6b8:0:51e:fd68:81c0:5d34:4531, 2a02:6b8:c12:422b:0:41c8:85ec:0",
            }
        ),
    )
    resp_data = await resp.json()
    assert resp.status == 200, resp_data
