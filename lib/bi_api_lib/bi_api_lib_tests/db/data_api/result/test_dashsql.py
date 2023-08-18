from __future__ import annotations

import logging

import pytest

from bi_testing.test_data.sql_queries import (
    CH_QUERY, CH_QUERY_FULL, MSSQL_QUERY_FULL, MYSQL_QUERY_FULL, PG_QUERY_FULL, ORACLE_QUERY_FULL,
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
async def test_pg_dashsql_extended_result(async_api_local_env_low_level_client, pg_subselectable_connection_id):
    data_api_aio = async_api_local_env_low_level_client
    conn_id = pg_subselectable_connection_id

    resp = await data_api_aio.request(
        "post", f"/api/v1/connections/{conn_id}/dashsql",
        json={"sql_query": PG_QUERY_FULL})
    resp_data = await resp.json()
    assert resp.status == 200, resp_data
    assert resp_data[0]["data"]["names"] == [
        "number", "str", "num_bool", "num_bytea", "num_char",
        "num_int8", "num_int2", "num_int4",
        "num_text", "num_oid", "num_json",
        "num_float4", "num_float8", "num_numeric", "num_interval",
        "num_varchar", "num_date", "num_time", "num_timestamp", "num_timestamptz",
        "num_array", "some_nan",
    ]
    assert resp_data[0]["data"]["bi_types"] == [
        "integer", "string", "boolean", "unsupported", "string",
        "integer", "integer", "integer",
        "string", "unsupported", "unsupported",
        "float", "float", "float", "unsupported",
        "string", "date", "unsupported", "genericdatetime", "genericdatetime",
        "array_int", "float",
    ]
    assert resp_data[1]['data'][21] == 'nan', "must not be a float('nan')"


EXAMPLE_PARAMS = {
    "some_string": {"type_name": "string", "value": "some\\:string\nwith\\stuff"},
    "some_integer": {"type_name": "integer", "value": "562949953421312"},
    "some_float": {"type_name": "float", "value": "73786976294838206464.5"},
    "some_boolean": {"type_name": "boolean", "value": "true"},
    "some_other_boolean": {"type_name": "boolean", "value": "false"},
    "some_date": {"type_name": "date", "value": "2021-07-19"},
    "some_datetime": {"type_name": "datetime", "value": "2021-07-19T19:35:43"},
    "3xtr4 ше1гd param": {"type_name": "string", "value": "11"},
    "3xtr4 же1гd param": {"type_name": "string", "value": "22"},
    "intvalues": {"type_name": "integer", "value": ["1", "2", "3"]},
    "strvalues": {"type_name": "string", "value": ["a", "b", "c"]},
}


PG_QUERY_WITH_PARAMS = r"""
select
  'normal '':string'''::text as v1_normal_string,
  E'extended:string\nwith\nnewlines'::text as v2_ext_string,
  {{some_string}}::text as v3_param_string,
  {{some_integer}} as v4_param_integer,
  {{some_float}} as v5_param_float,
  {{some_boolean}} as v6_param_boolean,
  {{some_other_boolean}} as v7_param_boolean,
  {{some_date}} as v8_param_date,
  {{some_datetime}} as v9_param_datetime,
  {{3xtr4 ше1гd param}}::text as v10_weird_name,
  {{3xtr4 же1гd param}}::text as v11_weird_name,
  1 in {{intvalues}} as v12_int_in,
  0 in {{intvalues}} as v13_int_in_2,
  'a' in {{strvalues}} as v14_str_in,
  'z' in {{strvalues}} as v14_str_in_2,
  1 as stuff
"""


@pytest.mark.asyncio
async def test_pg_dashsql_params_result(async_api_local_env_low_level_client, pg_subselectable_connection_id):
    data_api_aio = async_api_local_env_low_level_client
    conn_id = pg_subselectable_connection_id

    resp = await data_api_aio.request(
        "post", f"/api/v1/connections/{conn_id}/dashsql",
        json={
            "sql_query": PG_QUERY_WITH_PARAMS,
            "params": EXAMPLE_PARAMS,
        },
    )
    resp_data = await resp.json()
    assert resp.status == 200, resp_data

    assert len(resp_data) == 3, resp_data
    assert resp_data[0]["event"] == "metadata", resp_data
    assert resp_data[0]["data"]["bi_types"] == [
        'string', 'string', 'string', 'integer', 'float', 'boolean', 'boolean',
        'date', 'genericdatetime', 'string', 'string',
        'boolean', 'boolean', 'boolean', 'boolean',
        'integer',
    ], resp_data[0]["data"]
    assert resp_data[1]["event"] == "row"
    assert resp_data[1]["data"] == [
        "normal ':string'", 'extended:string\nwith\nnewlines',
        'some\\:string\nwith\\stuff', 562949953421312, '73786976294838210000',
        True, False, '2021-07-19', '2021-07-19T19:35:43', '11', '22',
        True, False, True, False,
        1,
    ], resp_data[1]["data"]
    assert resp_data[2]["event"] == "footer"


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
async def test_mysql_dashsql_result(async_api_local_env_low_level_client, mysql_connection_id):
    data_api_aio = async_api_local_env_low_level_client
    conn_id = mysql_connection_id

    resp = await data_api_aio.request(
        "post", f"/api/v1/connections/{conn_id}/dashsql",
        json={"sql_query": MYSQL_QUERY_FULL})
    resp_data = await resp.json()
    assert resp_data[0]["event"] == "metadata", resp_data
    assert resp_data[0]["data"]["names"] == [
        "num", "num_unsigned", "num_signed",
        "num_decimal", "num_decimal_12_2",
        "num_text", "num_binary", "num_char",
        "num_date", "num_datetime",
        "num_nchar", "num_time", "int_30bit", "some_double",
    ]
    assert resp_data[0]["data"]["driver_types"] == [
        8, 8, 8,
        246, 246,
        253, 253, 253,
        10, 12,
        253, 11, 8, 5,
    ]
    assert resp_data[0]["data"]["db_types"] == [
        "bigint", "bigint", "bigint",
        "decimal", "decimal",
        "text", "text", "text",
        "date", "datetime",
        "text", "time", "bigint", "double",
    ]
    assert resp_data[0]["data"]["bi_types"] == [
        "integer", "integer", "integer",
        "float", "float",
        "string", "string", "string",
        "date", "genericdatetime",
        "string", "unsupported", "integer", "float",
    ]


MYSQL_QUERY_WITH_PARAMS = r"""
select
  'normal '':string''' as v1_normal_string,
  'extended:string\nwith\nnewlines' as v2_ext_string,
  {{some_string}} as v3_param_string,
  {{some_integer}} as v4_param_integer,
  {{some_float}} as v5_param_float,
  {{some_boolean}} as v6_param_boolean,
  {{some_other_boolean}} as v7_param_boolean,
  {{some_date}} as v8_param_date,
  {{some_datetime}} as v9_param_datetime,
  {{3xtr4 ше1гd param}} as v10_weird_name,
  {{3xtr4 же1гd param}} as v11_weird_name,
  1 in {{intvalues}} as v12_int_in,
  0 in {{intvalues}} as v13_int_in_2,
  'a' in {{strvalues}} as v14_str_in,
  'z' in {{strvalues}} as v14_str_in_2,
  1 as stuff
"""


@pytest.mark.asyncio
async def test_mysql_dashsql_params_result(async_api_local_env_low_level_client, mysql_connection_id):
    data_api_aio = async_api_local_env_low_level_client
    conn_id = mysql_connection_id

    resp = await data_api_aio.request(
        "post", f"/api/v1/connections/{conn_id}/dashsql",
        json={
            "sql_query": MYSQL_QUERY_WITH_PARAMS,
            "params": EXAMPLE_PARAMS,
        },
    )
    resp_data = await resp.json()
    assert resp.status == 200, resp_data

    assert len(resp_data) == 3, resp_data
    assert resp_data[0]["event"] == "metadata", resp_data
    assert resp_data[0]["data"]["bi_types"] == [
        'string', 'string', 'string', 'integer', 'float', 'integer', 'integer',
        'date', 'genericdatetime', 'string', 'string',
        'integer', 'integer', 'integer', 'integer',
        'integer',
    ], resp_data[0]["data"]
    assert resp_data[1]["event"] == "row"
    assert resp_data[1]["data"] == pytest.approx([
        "normal ':string'", 'extended:string\nwith\nnewlines',
        'some\\:string\nwith\\stuff', 562949953421312, 73786976294838206464.5,
        1, 0, '2021-07-19', '2021-07-19T19:35:43', '11', '22',
        1, 0, 1, 0,
        1,
    ]), resp_data[1]["data"]
    assert resp_data[2]["event"] == "footer"


@pytest.mark.asyncio
async def test_mssql_dashsql_result(async_api_local_env_low_level_client, mssql_connection_id):
    data_api_aio = async_api_local_env_low_level_client
    conn_id = mssql_connection_id

    resp = await data_api_aio.request(
        "post", f"/api/v1/connections/{conn_id}/dashsql",
        json={"sql_query": MSSQL_QUERY_FULL})
    resp_data = await resp.json()
    assert resp.status == 200, resp_data
    assert resp_data[0]["event"] == "metadata", resp_data
    assert resp_data[0]["data"]["names"] == [
        "number", "num_tinyint", "num_smallint", "num_integer", "num_bigint",
        "num_float", "num_real", "num_numeric", "num_decimal", "num_bit",
        "num_char", "num_varchar", "num_text", "num_nchar", "num_nvarchar", "num_ntext",
        "num_date", "num_datetime", "num_datetime2", "num_smalldatetime",
        "num_datetimeoffset", "uuid",
    ]
    assert resp_data[0]["data"]["driver_types"] == [
        "int", "int", "int", "int", "int",
        "float", "float", "decimal", "decimal", "bool",
        "str", "str", "str", "str", "str", "str",
        "str", "datetime", "str", "datetime",
        "str", "str",
    ]
    assert resp_data[0]["data"]["db_types"] == [
        "integer", "integer", "integer", "integer", "integer",
        "float", "float", "decimal", "decimal", "bit",
        "ntext", "ntext", "ntext", "ntext", "ntext", "ntext",
        "ntext", "datetime", "ntext", "datetime",
        "ntext", "ntext",
    ]
    assert resp_data[0]["data"]["bi_types"] == [
        "integer", "integer", "integer", "integer", "integer",
        "float", "float", "float", "float", "boolean",
        "string", "string", "string", "string", "string", "string",
        "string", "genericdatetime", "string", "genericdatetime",
        "string", "string",
    ]


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
        json={"sql_query": ORACLE_QUERY_WITH_PARAMS, "params": EXAMPLE_PARAMS})
    resp_data = await resp.json()
    assert resp.status == 200, resp_data


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'dashsql_params_types',
    [
        'string',
        'datetime',
    ],
)
async def test_prometheus_dashsql_result(
    async_api_local_env_low_level_client,
    promql_subselectable_connection_id,
    dashsql_params_types,
):
    data_api_aio = async_api_local_env_low_level_client
    conn_id = promql_subselectable_connection_id

    resp = await data_api_aio.request(
        "post", f"/api/v1/connections/{conn_id}/dashsql",
        json={
            "sql_query": '{job="nodeexporter", type={{data_type}}}',
            "params": {
                "from": {"type_name": dashsql_params_types, "value": "2021-09-28T17:00:00Z"},
                "to": {"type_name": dashsql_params_types, "value": "2021-09-28T18:00:00Z"},
                "step": {"type_name": "string", "value": "5m"},
                "data_type": {"type_name": "string", "value": "test_data"},
            },
        },
    )
    resp_data = await resp.json()
    assert resp.status == 200, resp_data


@pytest.mark.asyncio
async def test_prometheus_dashsql_result_with_connector_specific_params(
    async_api_local_env_low_level_client,
    promql_subselectable_connection_id,
):
    data_api_aio = async_api_local_env_low_level_client
    conn_id = promql_subselectable_connection_id

    resp = await data_api_aio.request(
        "post", f"/api/v1/connections/{conn_id}/dashsql",
        json={
            "sql_query": '{job="nodeexporter", type={{data_type}}}',
            "params": {
                "data_type": {"type_name": "string", "value": "test_data"},
            },
            "connector_specific_params": {
                "step": "5m",
                "from": "2021-09-28T17:00:00Z",
                "to": "2021-09-28T18:00:00Z",
            },
        },
    )
    resp_data = await resp.json()
    assert resp.status == 200, resp_data
