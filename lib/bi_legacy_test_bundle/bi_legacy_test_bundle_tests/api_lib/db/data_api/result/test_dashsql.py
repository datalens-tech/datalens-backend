from __future__ import annotations

import logging

import pytest

from dl_testing.test_data.sql_queries import DASHSQL_EXAMPLE_PARAMS

from bi_testing_ya.sql_queries import (
    CH_QUERY,
    CH_QUERY_FULL,
    PG_QUERY_FULL,
    ORACLE_QUERY_FULL,
)

LOGGER = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_ch_dashsql_result(async_api_local_env_low_level_client, ch_subselectable_connection_id):
    data_api_aio = async_api_local_env_low_level_client
    conn_id = ch_subselectable_connection_id

    resp = await data_api_aio.request(
        "post", f"/api/v1/connections/{conn_id}/dashsql",
        json={"sql_query": CH_QUERY})
    resp_data = await resp.json()
    assert resp.status == 200, resp_data

    assert resp_data[0]["event"] == "metadata", resp_data
    assert resp_data[0]["data"]["names"] == ["a", "b", "ts"]
    assert resp_data[0]["data"]["driver_types"] == ["Nullable(UInt8)", "Array(UInt8)", "Nullable(DateTime('UTC'))"]
    assert resp_data[0]["data"]["db_types"] == ["uint8", "array", "datetime"]
    assert resp_data[0]["data"]["bi_types"] == ["integer", "unsupported", "genericdatetime"]

    assert resp_data[0]["data"]["clickhouse_headers"]["X-ClickHouse-Timezone"] == "UTC", resp_data
    assert resp_data[1] == {"event": "row", "data": [11, [33, 44], "2020-01-02 03:04:16"]}, resp_data
    assert resp_data[2] == {"event": "row", "data": [22, [33, 44], "2020-01-02 03:04:27"]}, resp_data
    assert resp_data[-1]["event"] == "footer", resp_data
    assert isinstance(resp_data[-1]["data"], dict)


@pytest.mark.asyncio
async def test_ch_dashsql_extended_result(async_api_local_env_low_level_client, ch_subselectable_connection_id):
    data_api_aio = async_api_local_env_low_level_client
    conn_id = ch_subselectable_connection_id

    resp = await data_api_aio.request(
        "post", f"/api/v1/connections/{conn_id}/dashsql",
        json={"sql_query": CH_QUERY_FULL})
    resp_data = await resp.json()
    assert resp.status == 200, resp_data


@pytest.mark.asyncio
async def test_dashsql_disallowed_result(async_api_local_env_low_level_client, static_connection_id):
    data_api_aio = async_api_local_env_low_level_client
    conn_id = static_connection_id
    req_data = {"sql_query": CH_QUERY}
    resp = await data_api_aio.request(
        "post", f"/api/v1/connections/{conn_id}/dashsql",
        json=req_data)
    resp_data = await resp.json()
    assert resp.status == 400
    assert resp_data


@pytest.mark.asyncio
async def test_dashsql_invalid_params(async_api_local_env_low_level_client, ch_subselectable_connection_id):
    data_api_aio = async_api_local_env_low_level_client
    conn_id = ch_subselectable_connection_id
    req_data = {
        "sql_query": r"SELECT {{date}}",
        "params": {
            "date": {
                "type_name": "date",
                "value": "Invalid date",
            },
        },
    }
    resp = await data_api_aio.request(
        "post", f"/api/v1/connections/{conn_id}/dashsql",
        json=req_data)
    resp_data = await resp.json()
    assert resp.status == 400
    assert resp_data


@pytest.mark.asyncio
async def test_dashsql_invalid_params_format(async_api_local_env_low_level_client, ch_subselectable_connection_id):
    data_api_aio = async_api_local_env_low_level_client
    conn_id = ch_subselectable_connection_id
    req_data = {
        "sql_query": r"SELECT 'some_{{value}}'",
        "params": {
            "value": {
                "type_name": "string",
                "value": "value",
            },
        },
    }
    resp = await data_api_aio.request(
        "post", f"/api/v1/connections/{conn_id}/dashsql",
        json=req_data)
    resp_data = await resp.json()
    assert resp.status == 400
    assert resp_data


@pytest.mark.asyncio
async def test_dashsql_invalid_alias(async_api_local_env_low_level_client, ch_subselectable_connection_id):
    """
    Clickhouse doesn't support unicode aliases
    https://clickhouse.com/docs/en/sql-reference/syntax#identifiers
    """
    data_api_aio = async_api_local_env_low_level_client
    conn_id = ch_subselectable_connection_id
    req_data = {
        "sql_query": r"SELECT 1 AS русский текст",
    }
    resp = await data_api_aio.request(
        "post",
        f"/api/v1/connections/{conn_id}/dashsql",
        json=req_data,
    )
    resp_data = await resp.json()
    assert resp.status == 400
    assert resp_data


@pytest.mark.asyncio
async def test_pg_dashsql_params_cache(async_api_local_env_low_level_client, pg_subselectable_connection_id):
    """
    Ensure bindparams participate in the cache key.

    TODO: test that caches are actually enabled.
    """
    data_api_aio = async_api_local_env_low_level_client
    conn_id = pg_subselectable_connection_id

    query = 'select {{some_string}}::text as res'

    resp = await data_api_aio.request(
        "post", f"/api/v1/connections/{conn_id}/dashsql",
        json={
            "sql_query": query,
            "params": {"some_string": {"type_name": "string", "value": "v1"}},
        },
    )
    resp_data = await resp.json()
    assert resp.status == 200, resp_data
    assert resp_data[1]["data"][0] == "v1"

    resp = await data_api_aio.request(
        "post", f"/api/v1/connections/{conn_id}/dashsql",
        json={
            "sql_query": query,
            "params": {"some_string": {"type_name": "string", "value": "v2"}},
        },
    )
    resp_data = await resp.json()
    assert resp.status == 200, resp_data
    assert resp_data[1]["data"][0] == "v2"


@pytest.mark.asyncio
async def test_greenplum_dashsql_result(async_api_local_env_low_level_client, greenplum_connection_id):
    data_api_aio = async_api_local_env_low_level_client
    conn_id = greenplum_connection_id

    resp = await data_api_aio.request(
        "post", f"/api/v1/connections/{conn_id}/dashsql",
        json={"sql_query": PG_QUERY_FULL})
    resp_data = await resp.json()
    assert resp.status == 200, resp_data


@pytest.mark.asyncio
async def test_oracle_dashsql_result(async_api_local_env_low_level_client, oracle_connection_id):
    data_api_aio = async_api_local_env_low_level_client
    conn_id = oracle_connection_id

    resp = await data_api_aio.request(
        "post", f"/api/v1/connections/{conn_id}/dashsql",
        json={"sql_query": ORACLE_QUERY_FULL})
    resp_data = await resp.json()
    assert resp.status == 200, resp_data
    assert resp_data[0]["event"] == "metadata", resp_data
    assert resp_data[0]["data"]["names"] == [
        "NUM", "NUM_STR", "NUM_INTEGER", "NUM_NUMBER",
        "NUM_BINARY_FLOAT", "NUM_BINARY_DOUBLE",
        "NUM_CHAR", "NUM_VARCHAR", "NUM_VARCHAR2", "NUM_NCHAR", "NUM_NVARCHAR2",
        "NUM_DATE", "NUM_TIMESTAMP", "NUM_TIMESTAMP_TZ",
    ]
    assert resp_data[0]["data"]["driver_types"] == [
        "db_type_number", "db_type_varchar", "db_type_number",
        "db_type_number", "db_type_binary_float", "db_type_binary_double",
        "db_type_char", "db_type_varchar", "db_type_varchar",
        "db_type_nchar", "db_type_nvarchar", "db_type_date",
        "db_type_timestamp", "db_type_timestamp_tz",
    ]
    assert resp_data[0]["data"]["db_types"] == [
        "integer", "varchar", "integer", "integer",
        "binary_float", "binary_double",
        "char", "varchar", "varchar", "nchar", "nvarchar",
        "date", "timestamp", "timestamp",
    ]
    assert resp_data[0]["data"]["bi_types"] == [
        "integer", "string", "integer", "integer",
        "float", "float",
        "string", "string", "string", "string", "string",
        "genericdatetime", "genericdatetime", "genericdatetime",
    ]


ORACLE_QUERY_WITH_PARAMS = r"""
select
  'normal '':string''' as v1_normal_string,
  'extended:string' || chr(10) || 'with' || chr(10) || 'newlines' as v2_ext_string,
  {{some_string}} as v3_param_string,
  {{some_integer}} as v4_param_integer,
  {{some_float}} as v5_param_float,
  {{some_boolean}} as v6_param_boolean,
  {{some_other_boolean}} as v7_param_boolean,
  {{some_date}} as v8_param_date,
  {{some_datetime}} as v9_param_datetime,
  {{3xtr4 ше1гd param}} as v10_weird_name,
  {{3xtr4 же1гd param}} as v11_weird_name,
  case when 1 in (1, 2) then 1 else 0 end as v12_int_in_tst,
  case when 1 in {{intvalues}} then 1 else 0 end as v12_int_in,
  case when 0 in {{intvalues}} then 1 else 0 end as v13_int_in_2,
  case when 'a' in ('z', 'x') then 1 else 0 end as v14_str_in_tst,
  case when 'a' in {{strvalues}} then 1 else 0 end as v14_str_in,
  case when 'z' in {{strvalues}} then 1 else 0 end as v14_str_in_2,
  1 as stuff
FROM dual
"""


@pytest.mark.asyncio
async def test_oracle_dashsql_params_result(async_api_local_env_low_level_client, oracle_connection_id):
    data_api_aio = async_api_local_env_low_level_client
    conn_id = oracle_connection_id

    resp = await data_api_aio.request(
        "post", f"/api/v1/connections/{conn_id}/dashsql",
        json={"sql_query": ORACLE_QUERY_WITH_PARAMS, "params": DASHSQL_EXAMPLE_PARAMS})
    resp_data = await resp.json()
    assert resp.status == 200, resp_data
