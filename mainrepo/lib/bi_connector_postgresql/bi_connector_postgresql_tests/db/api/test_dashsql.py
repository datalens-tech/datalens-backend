from aiohttp.test_utils import TestClient
import pytest

from bi_api_lib_testing.connector.dashsql_suite import DefaultDashSQLTestSuite
from bi_testing.test_data.sql_queries import DASHSQL_EXAMPLE_PARAMS

from bi_connector_postgresql_tests.db.api.base import PostgreSQLDashSQLConnectionTest
from bi_connector_postgresql_tests.db.config import (
    DASHSQL_QUERY,
    QUERY_WITH_PARAMS,
    SUBSELECT_QUERY_FULL,
)


class TestPostgresDashSQL(PostgreSQLDashSQLConnectionTest, DefaultDashSQLTestSuite):
    @pytest.mark.asyncio
    async def test_result(self, data_api_lowlevel_aiohttp_client: TestClient, saved_connection_id: str):
        resp = await self.get_dashsql_response(
            data_api_aio=data_api_lowlevel_aiohttp_client,
            conn_id=saved_connection_id,
            query=DASHSQL_QUERY,
        )

        resp_data = await resp.json()
        assert resp_data[0]["event"] == "metadata", resp_data
        assert resp_data[0]["data"]["names"] == ["aa", "bb", "cc", "dd", "ee", "ff"]
        assert resp_data[0]["data"]["driver_types"] == [25, 1007, 23, 1114, 1184, 17]
        assert resp_data[0]["data"]["postgresql_typnames"] == [
            "text",
            "_int4",
            "int4",
            "timestamp",
            "timestamptz",
            "bytea",
        ]
        assert resp_data[0]["data"]["db_types"] == [
            "text",
            "array(integer)",
            "integer",
            "timestamp",
            "timestamp",
            "bytea",
        ]
        assert resp_data[0]["data"]["bi_types"] == [
            "string",
            "array_int",
            "integer",
            "genericdatetime",
            "genericdatetime",
            "unsupported",
        ]

        assert resp_data[-1]["event"] == "footer", resp_data[-1]

    @pytest.mark.asyncio
    async def test_result_with_error(self, data_api_lowlevel_aiohttp_client: TestClient, saved_connection_id: str):
        resp = await self.get_dashsql_response(
            data_api_aio=data_api_lowlevel_aiohttp_client,
            conn_id=saved_connection_id,
            query="select 1/0",
            fail_ok=True,
        )

        assert resp.status == 400
        resp_data = await resp.json()
        assert resp_data["code"] == "ERR.DS_API.DB.ZERO_DIVISION"
        assert resp_data["message"] == "Division by zero."
        assert resp_data["details"] == {"db_message": "division by zero"}

    @pytest.mark.asyncio
    async def test_result_extended(self, data_api_lowlevel_aiohttp_client: TestClient, saved_connection_id: str):
        resp = await self.get_dashsql_response(
            data_api_aio=data_api_lowlevel_aiohttp_client,
            conn_id=saved_connection_id,
            query=SUBSELECT_QUERY_FULL,
        )

        resp_data = await resp.json()
        assert resp_data[0]["data"]["names"] == [
            "number",
            "str",
            "num_bool",
            "num_bytea",
            "num_char",
            "num_int8",
            "num_int2",
            "num_int4",
            "num_text",
            "num_oid",
            "num_json",
            "num_float4",
            "num_float8",
            "num_numeric",
            "num_interval",
            "num_varchar",
            "num_date",
            "num_time",
            "num_timestamp",
            "num_timestamptz",
            "num_array",
            "some_nan",
        ]
        assert resp_data[0]["data"]["bi_types"] == [
            "integer",
            "string",
            "boolean",
            "unsupported",
            "string",
            "integer",
            "integer",
            "integer",
            "string",
            "unsupported",
            "unsupported",
            "float",
            "float",
            "float",
            "unsupported",
            "string",
            "date",
            "unsupported",
            "genericdatetime",
            "genericdatetime",
            "array_int",
            "float",
        ]
        assert resp_data[1]["data"][21] == "nan", "must not be a float('nan')"

    @pytest.mark.asyncio
    async def test_result_with_params(self, data_api_lowlevel_aiohttp_client: TestClient, saved_connection_id: str):
        resp = await self.get_dashsql_response(
            data_api_aio=data_api_lowlevel_aiohttp_client,
            conn_id=saved_connection_id,
            query=QUERY_WITH_PARAMS,
            params=DASHSQL_EXAMPLE_PARAMS,
        )

        resp_data = await resp.json()
        assert len(resp_data) == 3, resp_data
        assert resp_data[0]["event"] == "metadata", resp_data
        assert resp_data[0]["data"]["bi_types"] == [
            "string",
            "string",
            "string",
            "integer",
            "float",
            "boolean",
            "boolean",
            "date",
            "genericdatetime",
            "string",
            "string",
            "boolean",
            "boolean",
            "boolean",
            "boolean",
            "integer",
        ], resp_data[0]["data"]
        assert resp_data[1]["event"] == "row"
        assert resp_data[1]["data"] == [
            "normal ':string'",
            "extended:string\nwith\nnewlines",
            "some\\:string\nwith\\stuff",
            562949953421312,
            "73786976294838210000",
            True,
            False,
            "2021-07-19",
            "2021-07-19T19:35:43",
            "11",
            "22",
            True,
            False,
            True,
            False,
            1,
        ], resp_data[1]["data"]
        assert resp_data[2]["event"] == "footer"
